"""
fipe_coleta.py — Coleta FIPE multi-veículo com asyncio + checkpoint
======================================================================
Estratégia de execução
  1. Carro  (tipo 1) — PRIORIDADE MÁXIMA, roda primeiro e de forma sequencial.
  2. Moto   (tipo 2) + Caminhão (tipo 3) — rodam em paralelo após carro finalizar.

Comportamento de datas (automático — ideal para rodar no dia 1 de cada mês)
  • Por padrão coleta APENAS a referência mais recente disponível na API FIPE.
  • Para coletar todo o histórico: FIPE_HISTORICO=true
  • Para um intervalo manual        : FIPE_REF_INICIO=<cod> e/ou FIPE_REF_FIM=<cod>

Cada tipo de veículo possui:
  • checkpoint independente  → fipe_checkpoint_<tipo>.json
  • log independente         → fipe_coleta_<label>.log
  • semáforo próprio         → evita que moto/caminhão disputem slots com carro

Variáveis de ambiente
  Obrigatórias : DB_USER, DB_PASS, DB_HOST, DB_NAME
  Opcionais    : DB_PORT                     (padrão 3306)
                 FIPE_HISTORICO              (padrão false — coleta só ref atual)
                 FIPE_REF_INICIO             (sobrescreve limite inferior)
                 FIPE_REF_FIM                (sobrescreve limite superior)
                 FIPE_CONCORRENCIA_CARRO     (padrão 15)
                 FIPE_CONCORRENCIA_MOTO      (padrão 10)
                 FIPE_CONCORRENCIA_CAMINHAO  (padrão 10)
                 FIPE_BATCH_SIZE             (padrão 200)

Ganho vs. versão sequencial dos notebooks: ~8-12×
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import time
import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import quote_plus

import aiohttp
import boto3
import pandas as pd
from sqlalchemy import create_engine, Engine

# ═══════════════════════════════════════════════════════════════════════════════
# Configurações globais
# ═══════════════════════════════════════════════════════════════════════════════

FIPE_URL = "http://veiculos.fipe.org.br/api/veiculos"
FIPE_HEADERS = {
    "cookie":       "ROUTEID=.5",
    "Host":         "veiculos.fipe.org.br",
    "Referer":      "http://veiculos.fipe.org.br",
    "Content-Type": "application/json",
    "User-Agent":   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36",
}

MAX_RETRIES = 8
BATCH_SIZE  = int(os.getenv("FIPE_BATCH_SIZE", "200"))

# Concorrência por tipo — 1 = serial (mais seguro contra bloqueio da API FIPE)
# Suba para 2-3 apenas se não estiver recebendo 403.
_CONC = {
    1: int(os.getenv("FIPE_CONCORRENCIA_CARRO",    "1")),
    2: int(os.getenv("FIPE_CONCORRENCIA_MOTO",     "1")),
    3: int(os.getenv("FIPE_CONCORRENCIA_CAMINHAO", "1")),
}

# Throttle global — intervalo mínimo (em segundos) entre quaisquer 2 requests
# à API FIPE. 1.0s é conservador e evita 403. Reduza por sua conta e risco.
REQUEST_DELAY = float(os.getenv("FIPE_REQUEST_DELAY", "1.0"))

# Mapeamento tipo_veiculo → metadados de tabelas e labels
@dataclass(frozen=True)
class VehicleConfig:
    tipo:        int
    label:       str          # nome amigável para logs
    tb_marca:    str          # tabela de marcas
    tb_modelo:   str          # tabela de modelos
    tb_ano:      str          # tabela de anos
    tb_preco:    str          # tabela de preços detalhados

VEHICLES: dict[int, VehicleConfig] = {
    1: VehicleConfig(
        tipo=1, label="Carro",
        tb_marca   = "fipe_marca_carro",
        tb_modelo  = "fipe_modelo_carro",
        tb_ano     = "fipe_modelo_ano_carro",
        tb_preco   = "fipe_modelo_ano_carro_versao_detalhado",
    ),
    2: VehicleConfig(
        tipo=2, label="Moto",
        tb_marca   = "fipe_marca_moto",
        tb_modelo  = "fipe_modelo_moto",
        tb_ano     = "fipe_modelo_ano_moto",
        tb_preco   = "fipe_modelo_ano_moto_versao_detalhado",
    ),
    3: VehicleConfig(
        tipo=3, label="Caminhão",
        tb_marca   = "fipe_marca_caminhao",
        tb_modelo  = "fipe_modelo_caminhao",
        tb_ano     = "fipe_modelo_ano_caminhao",
        tb_preco   = "fipe_modelo_ano_caminhao_versao_detalhado",
    ),
}

# ═══════════════════════════════════════════════════════════════════════════════
# Logging — configurado uma única vez; cada coletor usa seu próprio FileHandler
# ═══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler()],
)

def _make_logger(tipo: int) -> logging.Logger:
    label = VEHICLES[tipo].label
    logger = logging.getLogger(f"fipe.{label.lower()}")
    if not logger.handlers:
        fh = logging.FileHandler(f"fipe_coleta_{label.lower()}.log")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(fh)
    return logger

# ═══════════════════════════════════════════════════════════════════════════════
# Camada de persistência
#
# Destino definido por  --sink <valor>  (CLI, maior prioridade)
#                   ou  FIPE_SINK=<valor>  (env var, fallback)
#
#   s3    (padrão) → Parquet particionado no S3 — ideal para AWS Glue
#   excel           → .xlsx local — ideal para testar sem infra
#   mysql           → MySQL via SQLAlchemy (mantido para compat.)
#
# Variáveis para S3:
#   FIPE_S3_BUCKET   — obrigatório se sink=s3
#   FIPE_S3_PREFIX   — prefixo do caminho (padrão: fipe/raw/)
#   Credenciais AWS via env padrão boto3 ou IAM Role do Glue (zero config)
# ═══════════════════════════════════════════════════════════════════════════════

def _resolve_sink() -> str:
    """CLI --sink tem maior prioridade; FIPE_SINK é o fallback; padrão = s3."""
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--sink", default=None,
                        choices=["s3", "excel", "mysql"],
                        help="Destino dos dados: s3 | excel | mysql")
    args, _ = parser.parse_known_args(sys.argv[1:])
    return (args.sink or os.getenv("FIPE_SINK", "s3")).strip().lower()

FIPE_SINK = _resolve_sink()

# ── MySQL ─────────────────────────────────────────────────────────────────────

_ENGINE: Engine | None = None

def _build_engine() -> Engine:
    user  = os.environ["DB_USER"]
    passw = quote_plus(os.environ["DB_PASS"])
    host  = os.environ["DB_HOST"]
    port  = os.getenv("DB_PORT", "3306")
    db    = os.environ["DB_NAME"]
    return create_engine(
        f"mysql+pymysql://{user}:{passw}@{host}:{port}/{db}?charset=utf8",
        pool_size=10, max_overflow=20, echo=False,
    )

def get_engine() -> Engine:
    global _ENGINE
    if _ENGINE is None:
        _ENGINE = _build_engine()
    return _ENGINE

# ── Buffer compartilhado (S3 e Excel acumulam em memória) ─────────────────────
#
# Estrutura: { nome_da_tabela: [row_dict, ...] }
# Gravado de uma vez no final para evitar muitos arquivos pequenos no S3.

_BUFFER: dict[str, list[dict]] = defaultdict(list)

# ── S3 / Parquet ───────────────────────────────────────────────────────────────
#
# Layout no bucket (Hive-style — Glue Crawler detecta automático):
#
#   s3://<bucket>/<prefix>/<table>/data_carga=<ref>/<table>_<ts>.parquet
#
# Exemplo:
#   s3://meu-bucket/fipe/raw/fipe_modelo_ano_carro_versao_detalhado/
#       data_carga=320/
#           fipe_modelo_ano_carro_versao_detalhado_20260501_130000.parquet

S3_BUCKET = os.getenv("FIPE_S3_BUCKET", "")
S3_PREFIX = os.getenv("FIPE_S3_PREFIX", "fipe/raw/").rstrip("/") + "/"

def flush_s3(log: logging.Logger) -> None:
    """Grava os buffers no S3 como Parquet particionado por tabela e data_carga."""
    if not S3_BUCKET:
        raise EnvironmentError(
            "FIPE_S3_BUCKET não definido. "
            "Configure a variável de ambiente ou use --sink excel para teste local."
        )
    if not _BUFFER:
        log.warning("Nenhum dado acumulado para gravar no S3.")
        return

    s3 = boto3.client("s3")
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    for table, rows in _BUFFER.items():
        df = pd.DataFrame(rows)

        # Agrupa por data_carga para criar partições Hive
        grupos = df.groupby("data_carga") if "data_carga" in df.columns \
                 else [("all", df)]

        for ref_cod, grupo in grupos:
            parquet_bytes = grupo.to_parquet(index=False, engine="pyarrow")
            key = f"{S3_PREFIX}{table}/data_carga={ref_cod}/{table}_{ts}.parquet"
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=parquet_bytes)
            log.info("  [S3] s3://%s/%s (%d linhas)", S3_BUCKET, key, len(grupo))

# ── Excel ──────────────────────────────────────────────────────────────────────

def flush_excel(log: logging.Logger) -> None:
    """Grava os buffers em um arquivo .xlsx local (uma aba por tabela)."""
    if not _BUFFER:
        log.warning("Nenhum dado acumulado para gravar em Excel.")
        return

    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"fipe_coleta_{ts}.xlsx"

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:
        for table, rows in _BUFFER.items():
            df    = pd.DataFrame(rows)
            sheet = table[:31]          # limite de 31 chars do Excel
            df.to_excel(writer, sheet_name=sheet, index=False)
            log.info("  [Excel] %d linhas → aba '%s'", len(df), sheet)

    log.info("Arquivo gerado: %s", filename)

# ── Interface unificada ───────────────────────────────────────────────────────

def save_batch(rows: list[dict], table: str, log: logging.Logger) -> None:
    """Persiste um lote. MySQL grava imediatamente; S3 e Excel acumulam em buffer."""
    if not rows:
        return
    df = pd.DataFrame(rows)

    if FIPE_SINK in ("s3", "excel"):
        _BUFFER[table].extend(rows)
        log.info("  [buffer] %d linhas → '%s'", len(df), table)
    else:
        # MySQL — grava imediatamente
        df.to_sql(name=table, con=get_engine(), if_exists="append", index=False, chunksize=500)
        log.info("  → %d linhas → '%s'", len(df), table)


# ═══════════════════════════════════════════════════════════════════════════════
# Checkpoint
# ═══════════════════════════════════════════════════════════════════════════════

def _checkpoint_path(tipo: int) -> str:
    return f"fipe_checkpoint_{tipo}.json"

def load_checkpoint(tipo: int) -> set[str]:
    path = _checkpoint_path(tipo)
    if os.path.exists(path):
        with open(path) as f:
            return set(json.load(f))
    return set()

def save_checkpoint(tipo: int, done: set[str]) -> None:
    with open(_checkpoint_path(tipo), "w") as f:
        json.dump(list(done), f)

# ═══════════════════════════════════════════════════════════════════════════════
# Cliente HTTP assíncrono — throttle global para evitar bursts
# ═══════════════════════════════════════════════════════════════════════════════

# Lock global que garante intervalo mínimo entre requests.
# Criado sob demanda (lazy) para compatibilidade com Python 3.9,
# que vincula o Lock ao event loop ativo na criação.
_THROTTLE_LOCK: asyncio.Lock | None = None
_LAST_REQUEST: float = 0.0

async def _throttled_post(
    session: aiohttp.ClientSession,
    url: str,
    payload: dict,
) -> aiohttp.ClientResponse:
    """Faz POST respeitando intervalo mínimo global entre requests."""
    global _THROTTLE_LOCK, _LAST_REQUEST
    if _THROTTLE_LOCK is None:
        _THROTTLE_LOCK = asyncio.Lock()
    async with _THROTTLE_LOCK:
        elapsed = time.monotonic() - _LAST_REQUEST
        if elapsed < REQUEST_DELAY:
            await asyncio.sleep(REQUEST_DELAY - elapsed)
        _LAST_REQUEST = time.monotonic()
    # O POST em si roda fora do lock → permite sobreposição de I/O
    return await session.post(
        url, json=payload, headers=FIPE_HEADERS,
        timeout=aiohttp.ClientTimeout(total=30),
    )


async def _post(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    endpoint: str,
    payload: dict,
    log: logging.Logger,
) -> Any:
    url = f"{FIPE_URL}/{endpoint}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with sem:
                async with await _throttled_post(session, url, payload) as resp:
                    if resp.status in (429, 403):
                        wait = 15 + random.uniform(5, 15)
                        log.warning(
                            "HTTP %d em [%s] — aguardando %.0fs (tentativa %d/%d)",
                            resp.status, endpoint, wait, attempt, MAX_RETRIES,
                        )
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        raise ValueError(f"HTTP {resp.status}")
                    body = await resp.text()
                    if not body:
                        raise ValueError("Resposta vazia")
                    return json.loads(body)
        except Exception as exc:
            wait = min((2 ** attempt) + random.uniform(0, 2), 30)
            log.warning("Tentativa %d/%d [%s]: %s — aguard. %.1fs",
                        attempt, MAX_RETRIES, endpoint, exc, wait)
            if attempt == MAX_RETRIES:
                raise
            await asyncio.sleep(wait)
    return {}

# ═══════════════════════════════════════════════════════════════════════════════
# Funções de consulta FIPE (recebem tipo_veiculo explicitamente)
# ═══════════════════════════════════════════════════════════════════════════════

async def get_referencias(session, sem, log) -> pd.DataFrame:
    data = await _post(session, sem, "ConsultarTabelaDeReferencia", {}, log)
    return pd.DataFrame(data)

async def get_marcas(session, sem, ref: int, tipo: int, log) -> pd.DataFrame:
    data = await _post(session, sem, "ConsultarMarcas", {
        "codigoTabelaReferencia": ref,
        "codigoTipoVeiculo":      tipo,
    }, log)
    return pd.DataFrame(data)

async def get_modelos(session, sem, ref: int, tipo: int, marca: int, log) -> list:
    data = await _post(session, sem, "ConsultarModelos", {
        "codigoTabelaReferencia": ref,
        "codigoTipoVeiculo":      tipo,
        "codigoMarca":            marca,
    }, log)
    return data.get("Modelos", []) if isinstance(data, dict) else []

async def get_anos(session, sem, ref: int, tipo: int, marca: int, modelo: int, log) -> list:
    data = await _post(session, sem, "ConsultarAnoModelo", {
        "codigoTabelaReferencia": ref,
        "codigoTipoVeiculo":      tipo,
        "codigoMarca":            marca,
        "codigoModelo":           modelo,
    }, log)
    return data if isinstance(data, list) else []

async def get_preco(
    session, sem,
    ref: int, tipo: int, marca: int, modelo: int,
    ano_cod: str, ano: int, combustivel: int,
    log,
) -> dict:
    return await _post(session, sem, "ConsultarValorComTodosParametros", {
        "codigoTabelaReferencia": ref,
        "codigoTipoVeiculo":      tipo,
        "codigoMarca":            marca,
        "codigoModelo":           modelo,
        "ano":                    ano_cod,
        "codigoTipoCombustivel":  combustivel,
        "anoModelo":              ano,
        "tipoConsulta":           "tradicional",
    }, log)

# ═══════════════════════════════════════════════════════════════════════════════
# Worker: coleta todos os preços de um modelo
# ═══════════════════════════════════════════════════════════════════════════════

async def processar_modelo(
    session, sem,
    cfg: VehicleConfig,
    ref: int,
    marca_id: int, marca_label: str,
    modelo_id: int, modelo_label: str,
    done: set, lock: asyncio.Lock, counters: dict,
    log: logging.Logger,
) -> tuple[list, list]:

    try:
        anos = await get_anos(session, sem, ref, cfg.tipo, marca_id, modelo_id, log)
    except Exception as e:
        log.error("Erro anos %s/%s: %s", marca_label, modelo_label, e)
        return [], []

    if not anos:
        return [], []

    rows_anos:  list[dict] = []
    rows_preco: list[dict] = []

    for ano_item in anos:
        ano_cod = ano_item.get("Value", "")
        parts   = ano_cod.split("-") if isinstance(ano_cod, str) and "-" in ano_cod else [None, None]
        ano_veiculo = parts[0]
        combustivel = parts[1]
        ano_label   = ano_item.get("Label", "")

        rows_anos.append({
            "Value": ano_cod, "Label": ano_label,
            "data_carga":     ref,
            "tipo_veiculo":   cfg.tipo,
            "marca":          marca_id,
            "id_modelo":      modelo_id,
            "nome_modelo":    modelo_label,
            "cod_combustivel": combustivel,
            "ano_veiculo":    ano_veiculo,
        })

        chave = f"{cfg.tipo}_{ref}_{marca_id}_{modelo_id}_{ano_cod}"
        async with lock:
            if chave in done:
                continue  # já coletado → respeita checkpoint

        try:
            ano_int = int(ano_veiculo) if str(ano_veiculo).isdigit() else 32000
            preco   = await get_preco(
                session, sem,
                ref, cfg.tipo, marca_id, modelo_id,
                ano_cod, ano_int, int(combustivel),
                log,
            )
            preco.update({
                "data_carga":   ref,
                "tipo_veiculo": cfg.tipo,
                "id_marca":     marca_id,
                "id_modelo":    modelo_id,
                "nome_modelo":  modelo_label,
                "ano_codigo":   ano_cod,
                "ano":          ano_label,
            })
            rows_preco.append(preco)

            async with lock:
                done.add(chave)
                counters["total"] += 1
                if counters["total"] % 500 == 0:
                    save_checkpoint(cfg.tipo, done)
                    log.info(">> %d preços coletados (checkpoint salvo)", counters["total"])

        except Exception as e:
            log.error("Erro preço %s/%s/%s: %s", marca_label, modelo_label, ano_cod, e)

    return rows_anos, rows_preco

# ═══════════════════════════════════════════════════════════════════════════════
# Coleta de uma referência inteira (uma iteração mensal)
# ═══════════════════════════════════════════════════════════════════════════════

async def coleta_referencia(
    session, sem,
    cfg: VehicleConfig,
    ref: int, ref_label: str,
    done: set, lock: asyncio.Lock, counters: dict,
    log: logging.Logger,
) -> None:
    log.info("== Ref %d (%s)", ref, ref_label)

    df_marcas = await get_marcas(session, sem, ref, cfg.tipo, log)
    df_marcas["data_carga"] = ref
    save_batch(df_marcas.to_dict("records"), cfg.tb_marca, log)

    for _, marca_row in df_marcas.iterrows():
        marca_id    = marca_row["Value"]
        marca_label = marca_row["Label"]

        try:
            modelos = await get_modelos(session, sem, ref, cfg.tipo, marca_id, log)
        except Exception as e:
            log.error("Erro modelos %s: %s", marca_label, e)
            continue

        if not modelos:
            continue

        df_mod = pd.DataFrame(modelos)
        df_mod["data_carga"]   = ref
        df_mod["tipo_veiculo"] = cfg.tipo
        df_mod["marca"]        = marca_id
        save_batch(df_mod.to_dict("records"), cfg.tb_modelo, log)

        log.info("  %s — %d modelos (paralelo)", marca_label, len(modelos))

        tarefas = [
            processar_modelo(
                session, sem, cfg, ref,
                marca_id, marca_label,
                mod["Value"], mod["Label"],
                done, lock, counters, log,
            )
            for mod in modelos
        ]
        resultados = await asyncio.gather(*tarefas, return_exceptions=True)

        batch_anos:  list[dict] = []
        batch_preco: list[dict] = []

        for r in resultados:
            if isinstance(r, Exception):
                log.error("Tarefa falhou: %s", r)
                continue
            anos_rows, preco_rows = r
            batch_anos.extend(anos_rows)
            batch_preco.extend(preco_rows)

            if len(batch_preco) >= BATCH_SIZE:
                save_batch(batch_anos,  cfg.tb_ano,   log)
                save_batch(batch_preco, cfg.tb_preco, log)
                batch_anos, batch_preco = [], []

        save_batch(batch_anos,  cfg.tb_ano,   log)
        save_batch(batch_preco, cfg.tb_preco, log)

    save_checkpoint(cfg.tipo, done)

# ═══════════════════════════════════════════════════════════════════════════════
# Pipeline de coleta para um tipo de veículo completo
# ═══════════════════════════════════════════════════════════════════════════════

async def coleta_tipo(
    session: aiohttp.ClientSession,
    cfg: VehicleConfig,
    refs: list[tuple[int, str]],
    log: logging.Logger,
) -> None:
    """Executa a coleta completa para um tipo de veículo (sequencial por referência)."""

    sem      = asyncio.Semaphore(_CONC[cfg.tipo])
    done     = load_checkpoint(cfg.tipo)
    lock     = asyncio.Lock()
    counters = {"total": len(done)}

    if done:
        log.info("Checkpoint encontrado: %d chaves já processadas", len(done))

    t0 = time.time()

    for ref_cod, ref_label in refs:
        await coleta_referencia(
            session, sem, cfg,
            int(ref_cod), ref_label,
            done, lock, counters, log,
        )

    elapsed = time.time() - t0
    log.info("✔ %s concluído! %d preços em %.1f min",
             cfg.label, counters["total"], elapsed / 60)

# ═══════════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════════

async def main() -> None:
    root_log = logging.getLogger("fipe")
    root_log.info("=" * 60)
    root_log.info("FIPE Pipeline  |  carro → (moto ∥ caminhão)")
    root_log.info("=" * 60)

    # Um único semáforo compartilhado para buscar referências (baixo volume)
    sem_meta = asyncio.Semaphore(5)

    # Uma sessão HTTP compartilhada entre todos os tipos
    total_limit = sum(_CONC.values()) + 10
    connector   = aiohttp.TCPConnector(limit=total_limit)

    async with aiohttp.ClientSession(connector=connector) as session:

        # ── Tabela de referências (única, compartilhada) ──────────────────────
        df_refs = await get_referencias(session, sem_meta, root_log)
        save_batch(df_refs.to_dict("records"), "fipe_data_historico", root_log)

        ref_atual = int(df_refs["Codigo"].max())   # referência mais recente da API
        historico = os.getenv("FIPE_HISTORICO", "false").strip().lower() in ("1", "true", "yes")

        # Limites: env vars sobrescrevem qualquer padrão
        if historico:
            # Coleta todo o histórico disponível
            ref_min = int(os.getenv("FIPE_REF_INICIO", str(df_refs["Codigo"].min())))
            ref_max = int(os.getenv("FIPE_REF_FIM",    str(ref_atual)))
            root_log.info("Modo HISTÓRICO ativado (FIPE_HISTORICO=true)")
        else:
            # Padrão: apenas a referência do mês atual — ideal para rodar no dia 1
            ref_min = int(os.getenv("FIPE_REF_INICIO", str(ref_atual)))
            ref_max = int(os.getenv("FIPE_REF_FIM",    str(ref_atual)))

        refs = df_refs.loc[
            (df_refs["Codigo"] >= ref_min) & (df_refs["Codigo"] <= ref_max),
            ["Codigo", "Mes"],
        ].values.tolist()

        modo = "HISTÓRICO" if historico else "MÊS ATUAL"
        root_log.info(
            "%d referência(s) selecionada(s) [%s] | cod %d → %d",
            len(refs), modo, ref_min, ref_max,
        )

        # ── FASE 1: Carro (prioridade máxima, roda isolado) ──────────────────
        root_log.info("━" * 60)
        root_log.info("FASE 1 — Coletando CARROS (tipo 1)")
        root_log.info("━" * 60)
        await coleta_tipo(session, VEHICLES[1], refs, _make_logger(1))

        # ── FASE 2: Moto + Caminhão em paralelo ──────────────────────────────
        root_log.info("━" * 60)
        root_log.info("FASE 2 — Coletando MOTOS (tipo 2) ∥ CAMINHÕES (tipo 3)")
        root_log.info("━" * 60)
        await asyncio.gather(
            coleta_tipo(session, VEHICLES[2], refs, _make_logger(2)),
            coleta_tipo(session, VEHICLES[3], refs, _make_logger(3)),
        )

    # ── Flush final ───────────────────────────────────────────────────────────
    if FIPE_SINK == "s3":
        root_log.info("Enviando dados ao S3 (bucket=%s, prefix=%s)…", S3_BUCKET, S3_PREFIX)
        flush_s3(root_log)
    elif FIPE_SINK == "excel":
        root_log.info("Gravando Excel…")
        flush_excel(root_log)
    elif FIPE_SINK == "mysql" and _ENGINE:
        _ENGINE.dispose()

    root_log.info("=" * 60)
    root_log.info("Pipeline FIPE finalizado com sucesso! [sink=%s]", FIPE_SINK)
    root_log.info("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
