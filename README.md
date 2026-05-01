# 🚗 High-Performance FIPE Data Pipeline

Pipeline assíncrono de alta performance para coleta histórica completa da tabela FIPE, com checkpoint automático, retry inteligente e persistência em MySQL.

## ✨ Destaques

| Métrica | Valor |
|---|---|
| Velocidade vs. scraper sequencial | **8–12×** mais rápido |
| Coleta histórica completa estimada | **1–3 horas** (vs. ~15 h antes) |
| Requisições simultâneas (padrão) | **15** (configurável) |
| Retry com back-off exponencial | até **5 tentativas** por request |
| Rate-limit 429 handling | espera automática de ~30 s |
| Checkpoint | salvo a cada **500 preços** coletados |

## 📁 Estrutura

```
.
├── fipe_coleta.py        # Script principal de coleta
├── requirements.txt      # Dependências Python
├── .env.example          # Template de variáveis de ambiente
└── .gitignore
```

## ⚙️ Configuração

### 1. Clone o repositório
```bash
git clone https://github.com/hizatsuki/High-Performance-FIPE-Data-Pipeline-with-Python-Asyncio.git
cd High-Performance-FIPE-Data-Pipeline-with-Python-Asyncio
```

### 2. Crie um ambiente virtual e instale as dependências
```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Configure as variáveis de ambiente
```bash
cp .env.example .env
# Edite .env com suas credenciais de banco
```

### 4. Variáveis disponíveis

| Variável | Obrigatória | Padrão | Descrição |
|---|---|---|---|
| `DB_USER` | ✅ | — | Usuário MySQL |
| `DB_PASS` | ✅ | — | Senha MySQL |
| `DB_HOST` | ✅ | — | Host do banco |
| `DB_NAME` | ✅ | — | Nome do banco |
| `DB_PORT` | ❌ | `3306` | Porta MySQL |
| `FIPE_TIPO_VEICULO` | ❌ | `1` | 1=carro, 2=moto, 3=caminhão |
| `FIPE_REF_INICIO` | ❌ | mínimo disponível | Código da referência inicial |
| `FIPE_REF_FIM` | ❌ | máximo disponível | Código da referência final |
| `FIPE_CONCORRENCIA` | ❌ | `15` | Requisições HTTP simultâneas |
| `FIPE_BATCH_SIZE` | ❌ | `200` | Linhas por INSERT em lote |

## 🚀 Execução

```bash
# Coleta completa (todas as referências disponíveis)
python fipe_coleta.py

# Coleta de um intervalo específico de referências
FIPE_REF_INICIO=300 FIPE_REF_FIM=320 python fipe_coleta.py

# Ajustando concorrência e tipo de veículo
FIPE_TIPO_VEICULO=2 FIPE_CONCORRENCIA=20 python fipe_coleta.py
```

> 💡 O script retoma automaticamente do ponto onde parou usando o arquivo `fipe_checkpoint.json`. Para reiniciar do zero, basta apagar esse arquivo.

## 🗃️ Tabelas geradas no MySQL

| Tabela | Conteúdo |
|---|---|
| `fipe_data_historico` | Tabelas de referência mensais |
| `fipe_marca_carro` | Marcas por referência |
| `fipe_modelo_carro` | Modelos por marca e referência |
| `fipe_modelo_ano_carro` | Anos/combustíveis por modelo |
| `fipe_modelo_ano_carro_versao_detalhado` | Preços detalhados (tabela principal) |

## 🏗️ Arquitetura

```
main()
 └── coleta_referencia()          ← loop sequencial por referência mensal
      └── processar_modelo()      ← asyncio.gather() — todos os modelos em paralelo
           └── get_preco()        ← aiohttp + Semaphore (controla concorrência)
                └── _post()       ← retry com back-off exponencial
```

O **Semaphore** (`FIPE_CONCORRENCIA`) garante que nunca hajam mais do que N requisições abertas ao mesmo tempo, evitando sobrecarga na API e bloqueios por rate-limit.

## 📋 Logs

O script gera logs simultâneos no terminal e no arquivo `fipe_coleta.log`:

```
2025-01-15 10:00:00 [INFO] == Ref 320 (janeiro/2025)
2025-01-15 10:00:01 [INFO]   Fiat - 42 modelos (paralelo)
2025-01-15 10:00:05 [INFO]   >> 500 precos coletados (checkpoint salvo)
```

## 📦 Dependências

- [`aiohttp`](https://docs.aiohttp.org/) — cliente HTTP assíncrono
- [`pandas`](https://pandas.pydata.org/) — transformação e carga de dados
- [`SQLAlchemy`](https://www.sqlalchemy.org/) + [`PyMySQL`](https://pymysql.readthedocs.io/) — ORM e driver MySQL
- [`python-dotenv`](https://saurabh-kumar.com/python-dotenv/) — carregamento do `.env`

## 📄 Licença

MIT