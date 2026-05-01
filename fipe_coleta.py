"""
fipe_coleta.py — Coleta FIPE com requests assíncronas + checkpoint
Rodar: python fipe_coleta.py

Variáveis de ambiente:
  Obrigatórias : DB_USER, DB_PASS, DB_HOST, DB_NAME
  Opcionais    : DB_PORT (3306), FIPE_TIPO_VEICULO (1=carro),
                 FIPE_REF_INICIO, FIPE_REF_FIM,
                 FIPE_CONCORRENCIA (default 15),
                 FIPE_BATCH_SIZE   (default 200)

Ganho de velocidade: ~8-12x vs versão sequencial.
Estimativa: coleta completa histórica em 1-3h (vs 15h antes).
"""

import asyncio
import json
import logging
import os
import random
import time
from urllib.parse import quote_plus

import aiohttp
import pandas as pd
from sqlalchemy import create_engine

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("fipe_coleta.log")],
)
log = logging.getLogger(__name__)

FIPE_URL     = "http://veiculos.fipe.org.br/api/veiculos"
FIPE_HEADERS = {
    "cookie":       "ROUTEID=.5",
    "Host":         "veiculos.fipe.org.br",
    "Referer":      "http://veiculos.fipe.org.br",
    "Content-Type": "application/json",
}

TIPO_VEICULO = int(os.getenv("FIPE_TIPO_VEICULO", "1"))
CONCORRENCIA = int(os.getenv("FIPE_CONCORRENCIA", "15"))   # requests simultaneas
BATCH_SIZE   = int(os.getenv("FIPE_BATCH_SIZE",   "200"))  # linhas por INSERT
MAX_RETRIES  = 5
CHECKPOINT   = "fipe_checkpoint.json"


# ─────────────────────────────────────────────
# Banco
# ─────────────────────────────────────────────

def get_engine():
    user  = os.environ["DB_USER"]
    passw = quote_plus(os.environ["DB_PASS"])
    host  = os.environ["DB_HOST"]
    port  = os.getenv("DB_PORT", "3306")
    db    = os.environ["DB_NAME"]
    return create_engine(
        f"mysql+pymysql://{user}:{passw}@{host}:{port}/{db}?charset=utf8",
        pool_size=5, max_overflow=10, echo=False,
    )

ENGINE = None

def engine():
    global ENGINE
    if ENGINE is None:
        ENGINE = get_engine()
    return ENGINE


def save_batch(rows: list, table: str):
    if not rows:
        return
    df = pd.DataFrame(rows)
    df.to_sql(name=table, con=engine(), if_exists="append", index=False, chunksize=500)
    log.info("  -> %d linhas -> '%s'", len(df), table)


# ─────────────────────────────────────────────
# Checkpoint (retomar de onde parou)
# ─────────────────────────────────────────────

def load_checkpoint() -> set:
    """Retorna set de chaves ja processadas: '{ref}_{marca}_{modelo}_{ano_cod}'"""
    if os.path.exists(CHECKPOINT):
        with open(CHECKPOINT) as f:
            return set(json.load(f))
    return set()


def save_checkpoint(done: set):
    with open(CHECKPOINT, "w") as f:
        json.dump(list(done), f)


# ─────────────────────────────────────────────
# Cliente FIPE assincrono
# ─────────────────────────────────────────────

async def _post(session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                endpoint: str, payload: dict):
    url = f"{FIPE_URL}/{endpoint}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with sem:
                async with session.post(
                    url, json=payload, headers=FIPE_HEADERS,
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as resp:
                    if resp.status == 429:
                        wait = 30 + random.uniform(0, 5)
                        log.warning("  Rate-limit (429). Aguardando %.0fs...", wait)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        raise ValueError(f"HTTP {resp.status}")
                    body = await resp.text()
                    if not body:
                        raise ValueError("Resposta vazia")
                    return json.loads(body)
        except Exception as exc:
            wait = (2 ** attempt) + random.uniform(0, 1)
            log.warning("  Tentativa %d/%d [%s]: %s. Aguard. %.1fs",
                        attempt, MAX_RETRIES, endpoint, exc, wait)
            if attempt == MAX_RETRIES:
                raise
            await asyncio.sleep(wait)
    return {}


async def get_referencias(session, sem):
    data = await _post(session, sem, "ConsultarTabelaDeReferencia", {})
    return pd.DataFrame(data)


async def get_marcas(session, sem, ref: int):
    data = await _post(session, sem, "ConsultarMarcas", {
        "codigoTabelaReferencia": ref,
        "codigoTipoVeiculo": TIPO_VEICULO,
    })
    return pd.DataFrame(data)


async def get_modelos(session, sem, ref: int, marca: int) -> list:
    data = await _post(session, sem, "ConsultarModelos", {
        "codigoTabelaReferencia": ref,
        "codigoTipoVeiculo": TIPO_VEICULO,
        "codigoMarca": marca,
    })
    return data.get("Modelos", []) if isinstance(data, dict) else []


async def get_anos(session, sem, ref: int, marca: int, modelo: int) -> list:
    data = await _post(session, sem, "ConsultarAnoModelo", {
        "codigoTabelaReferencia": ref,
        "codigoTipoVeiculo": TIPO_VEICULO,
        "codigoMarca": marca,
        "codigoModelo": modelo,
    })
    return data if isinstance(data, list) else []


async def get_preco(session, sem, ref: int, marca: int, modelo: int,
                    ano_cod: str, ano: int, combustivel: int) -> dict:
    return await _post(session, sem, "ConsultarValorComTodosParametros", {
        "codigoTabelaReferencia": ref,
        "codigoTipoVeiculo": TIPO_VEICULO,
        "codigoMarca": marca,
        "codigoModelo": modelo,
        "ano": ano_cod,
        "codigoTipoCombustivel": combustivel,
        "anoModelo": ano,
        "tipoConsulta": "tradicional",
    })


# ─────────────────────────────────────────────
# Worker: coleta todos os precos de um modelo
# ─────────────────────────────────────────────

async def processar_modelo(
    session, sem, ref: int, marca_id: int, marca_label: str,
    modelo_id: int, modelo_label: str,
    done: set, lock: asyncio.Lock, counters: dict,
):
    try:
        anos = await get_anos(session, sem, ref, marca_id, modelo_id)
    except Exception as e:
        log.error("  Erro anos %s/%s: %s", marca_label, modelo_label, e)
        return [], []

    if not anos:
        return [], []

    rows_anos  = []
    rows_preco = []

    for ano_item in anos:
        ano_cod = ano_item.get("Value", "")
        parts   = ano_cod.split("-") if isinstance(ano_cod, str) and "-" in ano_cod else [None, None]
        ano_veiculo = parts[0]
        combustivel = parts[1]
        ano_label   = ano_item.get("Label", "")

        rows_anos.append({
            "Value": ano_cod, "Label": ano_label,
            "data_carga": ref, "tipo_veiculo": TIPO_VEICULO,
            "marca": marca_id, "id_modelo": modelo_id,
            "nome_modelo": modelo_label,
            "cod_combustivel": combustivel, "ano_veiculo": ano_veiculo,
        })

        chave = f"{ref}_{marca_id}_{modelo_id}_{ano_cod}"
        async with lock:
            if chave in done:
                continue  # ja coletado (checkpoint)

        try:
            ano_int = int(ano_veiculo) if str(ano_veiculo).isdigit() else 32000
            preco   = await get_preco(session, sem, ref, marca_id, modelo_id,
                                      ano_cod, ano_int, int(combustivel))
            preco.update({
                "data_carga": ref, "tipo_veiculo": TIPO_VEICULO,
                "id_marca": marca_id, "id_modelo": modelo_id,
                "nome_modelo": modelo_label,
                "ano_codigo": ano_cod, "ano": ano_label,
            })
            rows_preco.append(preco)

            async with lock:
                done.add(chave)
                counters["total"] += 1
                if counters["total"] % 500 == 0:
                    save_checkpoint(done)
                    log.info("  >> %d precos coletados (checkpoint salvo)", counters["total"])

        except Exception as e:
            log.error("  Erro preco %s/%s/%s: %s", marca_label, modelo_label, ano_cod, e)

    return rows_anos, rows_preco


# ─────────────────────────────────────────────
# Coleta de uma referencia inteira
# ─────────────────────────────────────────────

async def coleta_referencia(session, sem, ref: int, ref_label: str,
                            done: set, lock: asyncio.Lock, counters: dict):
    log.info("== Ref %d (%s)", ref, ref_label)

    df_marcas = await get_marcas(session, sem, ref)
    df_marcas["data_carga"] = ref
    save_batch(df_marcas.to_dict("records"), "fipe_marca_carro")

    for _, marca_row in df_marcas.iterrows():
        marca_id    = marca_row["Value"]
        marca_label = marca_row["Label"]

        try:
            modelos = await get_modelos(session, sem, ref, marca_id)
        except Exception as e:
            log.error("  Erro modelos %s: %s", marca_label, e)
            continue

        if not modelos:
            continue

        df_mod = pd.DataFrame(modelos)
        df_mod["data_carga"]   = ref
        df_mod["tipo_veiculo"] = TIPO_VEICULO
        df_mod["marca"]        = marca_id
        save_batch(df_mod.to_dict("records"), "fipe_modelo_carro")

        log.info("  %s - %d modelos (paralelo)", marca_label, len(modelos))

        # Todos os modelos da marca em paralelo (controlado pelo semaforo)
        tarefas = [
            processar_modelo(
                session, sem, ref, marca_id, marca_label,
                mod["Value"], mod["Label"],
                done, lock, counters,
            )
            for mod in modelos
        ]
        resultados = await asyncio.gather(*tarefas, return_exceptions=True)

        batch_anos  = []
        batch_preco = []
        for r in resultados:
            if isinstance(r, Exception):
                log.error("  Tarefa falhou: %s", r)
                continue
            anos_rows, preco_rows = r
            batch_anos.extend(anos_rows)
            batch_preco.extend(preco_rows)

            if len(batch_preco) >= BATCH_SIZE:
                save_batch(batch_anos,  "fipe_modelo_ano_carro")
                save_batch(batch_preco, "fipe_modelo_ano_carro_versao_detalhado")
                batch_anos, batch_preco = [], []

        save_batch(batch_anos,  "fipe_modelo_ano_carro")
        save_batch(batch_preco, "fipe_modelo_ano_carro_versao_detalhado")

    save_checkpoint(done)


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

async def main():
    log.info("=" * 50)
    log.info("FIPE coleta  |  tipo=%d  |  concorrencia=%d", TIPO_VEICULO, CONCORRENCIA)
    log.info("=" * 50)

    done     = load_checkpoint()
    lock     = asyncio.Lock()
    counters = {"total": len(done)}
    sem      = asyncio.Semaphore(CONCORRENCIA)

    if done:
        log.info("Checkpoint encontrado: %d chaves ja processadas", len(done))

    connector = aiohttp.TCPConnector(limit=CONCORRENCIA + 5)
    async with aiohttp.ClientSession(connector=connector) as session:

        df_refs = await get_referencias(session, sem)
        save_batch(df_refs.to_dict("records"), "fipe_data_historico")

        ref_min = int(os.getenv("FIPE_REF_INICIO", str(df_refs["Codigo"].min())))
        ref_max = int(os.getenv("FIPE_REF_FIM",    str(df_refs["Codigo"].max())))
        refs = df_refs.loc[
            (df_refs["Codigo"] >= ref_min) & (df_refs["Codigo"] <= ref_max),
            ["Codigo", "Mes"]
        ].values.tolist()

        log.info("%d referencias selecionadas (%d -> %d)", len(refs), ref_min, ref_max)
        t0 = time.time()

        for ref_cod, ref_label in refs:
            await coleta_referencia(session, sem, int(ref_cod), ref_label,
                                    done, lock, counters)

        elapsed = time.time() - t0
        log.info("=" * 50)
        log.info("Concluido! %d precos em %.1f min", counters["total"], elapsed / 60)
        log.info("=" * 50)

    if ENGINE:
        ENGINE.dispose()


if __name__ == "__main__":
    asyncio.run(main())
