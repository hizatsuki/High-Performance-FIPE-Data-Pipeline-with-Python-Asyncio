# 🚗🏍️🚚 High-Performance FIPE Data Pipeline

Pipeline assíncrono de alta performance para coleta histórica completa da tabela FIPE para **carros, motos e caminhões**, com execução priorizada, checkpoints independentes e persistência em MySQL.

## ✨ Destaques

| Métrica | Valor |
|---|---|
| Velocidade vs. scraper sequencial | **8–12×** mais rápido |
| Coleta histórica completa estimada | **1–3 horas** (vs. ~15 h antes) |
| Estratégia de execução | Carro primeiro → Moto ∥ Caminhão |
| Retry com back-off exponencial | até **5 tentativas** por request |
| Rate-limit 429 handling | espera automática de ~30 s |
| Checkpoint | por tipo, salvo a cada **500 preços** |

## 🗂️ Estratégia de execução

```
FASE 1  ──►  Carro (tipo 1)         prioridade máxima, roda isolado
FASE 2  ──►  Moto (tipo 2)
             Caminhão (tipo 3)       ← rodam em paralelo após carro finalizar
```

Cada tipo de veículo possui seu próprio:
- **Semáforo** → controla concorrência individualmente
- **Checkpoint** → `fipe_checkpoint_1.json`, `fipe_checkpoint_2.json`, `fipe_checkpoint_3.json`
- **Log** → `fipe_coleta_carro.log`, `fipe_coleta_moto.log`, `fipe_coleta_caminhão.log`

## 📁 Estrutura

```
.
├── fipe_coleta.py              # Pipeline principal (carro + moto + caminhão)
├── Fipe_coleta.ipynb           # Notebook legado — moto (referência)
├── Fipe_coleta_Caminhao.ipynb  # Notebook legado — caminhão (referência)
├── requirements.txt
├── .env.example
└── .gitignore
```

## ⚙️ Configuração

### 1. Clone o repositório
```bash
git clone https://github.com/hizatsuki/High-Performance-FIPE-Data-Pipeline-with-Python-Asyncio.git
cd High-Performance-FIPE-Data-Pipeline-with-Python-Asyncio
```

### 2. Ambiente virtual e dependências
```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Variáveis de ambiente
```bash
cp .env.example .env
# Edite .env com suas credenciais
```

### 4. Variáveis disponíveis

| Variável | Obrigatória | Padrão | Descrição |
|---|---|---|---|
| `DB_USER` | ✅ | — | Usuário MySQL |
| `DB_PASS` | ✅ | — | Senha MySQL |
| `DB_HOST` | ✅ | — | Host do banco |
| `DB_NAME` | ✅ | — | Nome do banco |
| `DB_PORT` | ❌ | `3306` | Porta MySQL |
| `FIPE_REF_INICIO` | ❌ | mínimo disponível | Código referência inicial |
| `FIPE_REF_FIM` | ❌ | máximo disponível | Código referência final |
| `FIPE_CONCORRENCIA_CARRO` | ❌ | `15` | Requests simultâneos — carro |
| `FIPE_CONCORRENCIA_MOTO` | ❌ | `10` | Requests simultâneos — moto |
| `FIPE_CONCORRENCIA_CAMINHAO` | ❌ | `10` | Requests simultâneos — caminhão |
| `FIPE_BATCH_SIZE` | ❌ | `200` | Linhas por INSERT em lote |

## 🚀 Execução

```bash
# Coleta completa (todas as referências, todos os tipos)
python fipe_coleta.py

# Apenas um intervalo de referências
FIPE_REF_INICIO=300 FIPE_REF_FIM=320 python fipe_coleta.py

# Ajustando concorrência
FIPE_CONCORRENCIA_CARRO=20 FIPE_CONCORRENCIA_MOTO=8 python fipe_coleta.py
```

> 💡 Para reiniciar um tipo do zero, apague o checkpoint correspondente:
> ```bash
> rm fipe_checkpoint_1.json   # carro
> rm fipe_checkpoint_2.json   # moto
> rm fipe_checkpoint_3.json   # caminhão
> ```

## 🗃️ Tabelas geradas no MySQL

| Tabela | Tipo | Conteúdo |
|---|---|---|
| `fipe_data_historico` | Todos | Tabelas de referência mensais |
| `fipe_marca_carro` | Carro | Marcas por referência |
| `fipe_modelo_carro` | Carro | Modelos por marca |
| `fipe_modelo_ano_carro` | Carro | Anos/combustíveis por modelo |
| `fipe_modelo_ano_carro_versao_detalhado` | Carro | **Preços detalhados** |
| `fipe_marca_moto` | Moto | Marcas por referência |
| `fipe_modelo_moto` | Moto | Modelos por marca |
| `fipe_modelo_ano_moto` | Moto | Anos/combustíveis por modelo |
| `fipe_modelo_ano_moto_versao_detalhado` | Moto | **Preços detalhados** |
| `fipe_marca_caminhao` | Caminhão | Marcas por referência |
| `fipe_modelo_caminhao` | Caminhão | Modelos por marca |
| `fipe_modelo_ano_caminhao` | Caminhão | Anos/combustíveis por modelo |
| `fipe_modelo_ano_caminhao_versao_detalhado` | Caminhão | **Preços detalhados** |

## 🏗️ Arquitetura

```
main()
 ├── FASE 1: coleta_tipo(carro)           ← await (bloqueante, prioridade)
 │     └── coleta_referencia()            ← loop sequencial por mês
 │          └── processar_modelo()        ← asyncio.gather() por marca
 │               └── get_preco()          ← aiohttp + Semaphore
 │                    └── _post()         ← retry back-off exponencial
 │
 └── FASE 2: asyncio.gather(
       coleta_tipo(moto),                 ← paralelo
       coleta_tipo(caminhão),             ← paralelo
     )
```

Cada `coleta_tipo` usa um **Semaphore** próprio, então moto e caminhão não disputam slots entre si nem com o carro.

## 📋 Logs

Logs simultâneos no terminal e em arquivos separados:

```
2025-01-15 10:00:00 [INFO] fipe — FASE 1 — Coletando CARROS (tipo 1)
2025-01-15 10:00:01 [INFO] fipe.carro — == Ref 320 (janeiro/2025)
2025-01-15 10:00:05 [INFO] fipe.carro — >> 500 preços coletados (checkpoint salvo)
2025-01-15 11:30:00 [INFO] fipe — FASE 2 — Coletando MOTOS ∥ CAMINHÕES
```

## 📦 Dependências

- [`aiohttp`](https://docs.aiohttp.org/) — cliente HTTP assíncrono
- [`pandas`](https://pandas.pydata.org/) — transformação e carga de dados
- [`SQLAlchemy`](https://www.sqlalchemy.org/) + [`PyMySQL`](https://pymysql.readthedocs.io/) — ORM e driver MySQL
- [`python-dotenv`](https://saurabh-kumar.com/python-dotenv/) — carregamento do `.env`

## 📄 Licença

MIT