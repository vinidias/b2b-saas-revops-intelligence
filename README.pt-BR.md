# Motor de Inteligência de RevOps para SaaS B2B

> **[🌐 Portal ao vivo](https://vinidias.github.io/b2b-saas-revops-intelligence/)** · **[📚 Documentação dbt](https://vinidias.github.io/b2b-saas-revops-intelligence/dbt_docs/)** · **[🛡️ Relatório de observabilidade](https://vinidias.github.io/b2b-saas-revops-intelligence/elementary_report.html)**

**Idiomas:** [English](README.md) · [Português (Brasil)](README.pt-BR.md) · [简体中文](README.zh-CN.md)

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=python&logoColor=white)
![Dagster](https://img.shields.io/badge/Dagster-163B36?style=for-the-badge&logo=python&logoColor=white)
![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)
![HubSpot](https://img.shields.io/badge/HubSpot-FF7A59?style=for-the-badge&logo=hubspot&logoColor=white)
![Stripe](https://img.shields.io/badge/Stripe-635BFF?style=for-the-badge&logo=stripe&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)

---

## Sumário

1. [Visão geral](#visão-geral)
2. [Contexto de negócio](#contexto-de-negócio)
3. [Arquitetura](#arquitetura)
4. [Ingestão (EL)](#1-ingestão-el)
5. [Transformação (dbt)](#2-transformação-dbt)
6. [Qualidade e observabilidade dos dados](#3-qualidade-e-observabilidade-dos-dados)
7. [Camada semântica e agente de IA no Slack](#4-camada-semântica-e-agente-de-ia-no-slack)
8. [Painéis de BI](#5-painéis-de-bi-painéis-como-código)
9. [Reverse ETL](#6-reverse-etl)
10. [Orquestração](#7-orquestração)
11. [Infraestrutura (Terraform)](#8-infraestrutura-terraform)
12. [CI/CD](#9-cicd)
13. [Início rápido](#início-rápido)
14. [Estrutura do repositório](#estrutura-do-repositório)
15. [Documentação relacionada](#documentação-relacionada)

---

## Visão geral

Um **pipeline de dados de Revenue Operations** pronto para produção que unifica dados de CRM, faturamento, suporte e produto em uma única fonte confiável e, em seguida, entrega automaticamente insights acionáveis às equipes de GTM.

| | |
|:-|:-|
| **Problema** | 4 ferramentas desconectadas (HubSpot · Stripe · Zendesk · PostHog), sem identidade compartilhada, churn silencioso e MRR pouco confiável |
| **Solução** | Pipeline ponta a ponta: ingestão → transformação → camada semântica → agente de IA no Slack → Reverse ETL para o CRM |
| **Resultado** | A primeira execução identificou **23 contas em risco (US$ 87 mil de ARR)**. A equipe de CS preservou **US$ 45 mil de ARR em 30 dias** |
| **Stack** | dlt · dbt · Snowflake · Dagster · Lightdash · Elementary · bot do Slack |
| **Custo de infraestrutura** | US$ 0/mês — plano gratuito do Snowflake e ferramentas de código aberto |

---

## Contexto de negócio

**[StackFlow AI](https://stackflow.ai)** — plataforma B2B SaaS de gestão de engenharia. ARR de US$ 3 milhões, Série A de US$ 10 milhões e mais de 60 funcionários. O crescimento acelerado expôs uma lacuna estrutural: cada equipe usava ferramentas excelentes que não se comunicavam.

| Equipe | Ponto cego |
|:-----|:-----------|
| Vendas (HubSpot) | Não conseguia saber se os negócios ganhos chegaram a ser ativados no produto |
| Finanças (Stripe) | Não tinha visibilidade sobre qual campanha ou representante gerou cada assinatura |
| Sucesso do Cliente (Zendesk) | Não conhecia o nível de engajamento dos produtos nas contas em risco |
| Produto (PostHog) | Não conseguia quantificar o risco de ARR associado ao baixo engajamento |

**Perdas concretas:**
- **Churn silencioso** — Uma conta Enterprise de US$ 12 mil/ano cancelou após 3 semanas de atraso no Stripe, 6 semanas sem atividade no Git e 4 chamados críticos em aberto. Cada equipe via um sinal. Ninguém via os três juntos.
- **Expansão perdida** — 30% das contas utilizavam mais de 80% das licenças. Enquanto isso, Vendas prospectava clientes frios.
- **MRR impreciso** — Finanças montava o relatório para o conselho manualmente: 16 horas por mês, sempre 2–3 semanas defasado, com erro de ±8%.
- **Gargalo de analistas** — Uma pergunta simples de segmentação levava 3 dias. Os dados existiam, mas estavam espalhados por 4 sistemas.

---

## Arquitetura

```
HubSpot · Stripe · Zendesk · PostHog · banco de dados interno
            ↓  (dlt — incremental, schema inferido)
         Snowflake RAW_DATA
            ↓  (dbt — arquitetura medalhão em 3 camadas)
    STAGING → INTERMEDIATE → MARTS
            ↓                      ↓
    Lightdash + IA no Slack   Reverse ETL → HubSpot
            ↓
       Dagster (orquestração diária às 07:00 UTC)
```

![Arquitetura completa de dados](screenshots/full_data_architecture.png)

<details>
<summary><strong>Stack tecnológico completo</strong></summary>

| Camada | Ferramenta | Função |
|:------|:-----|:-----|
| Ingestão | dlt | 5 conectores ativos → Snowflake; carga incremental e schema inferido |
| Transformação | dbt | SQL baseado em DAG, com testes, documentação e linhagem |
| Data warehouse | Snowflake | Data warehouse em nuvem |
| Observabilidade | Elementary | Detecção de anomalias e alertas no Slack |
| Camada semântica | Lightdash | Métricas como código a partir do YAML `meta` do dbt |
| Perguntas com IA | Lightdash AI + bot do Slack | Linguagem natural → Snowflake; sem necessidade de SQL |
| Orquestração | Dagster | DAG baseado em ativos: acompanha atualização dos dados, não apenas a execução de scripts |
| Reverse ETL | Python + dlt | Envia insights de volta ao HubSpot |
| CI/CD | GitHub Actions | Slim CI, verificações do Elementary e publicação automática da documentação dbt |

</details>

---

## 1. Ingestão (EL)

O **dlt** ingere 5 fontes no `RAW_DATA` do Snowflake, com inferência automática de schema, carregamento incremental e paginação.

| Fonte | Método | Tabelas principais |
|:-------|:-------|:-----------|
| HubSpot CRM | API REST + histórico de propriedades | `companies`, `contacts`, `deals` |
| Stripe Billing | API REST (cursor) | `subscriptions`, `invoices`, `customers` |
| Zendesk Support | API REST (incremental) | `tickets`, `users`, `organizations` |
| Eventos do PostHog | API REST | `events`, `persons` |
| Banco de dados interno | CDC do PostgreSQL (replicação lógica) | `users`, `workspaces`, `seats` |

**Modo de desenvolvimento:** `ingestion/stackflow_pipeline.py` — dados simulados → `DEV_RAW_DATA`  
**Modo de produção:** [`b2b_dlt/`](b2b_dlt/) — conectores de API ativos → `RAW_DATA`

![Saída da ingestão dlt](screenshots/dlt_load_snowflake_terminal.png)

---

## 2. Transformação (dbt)

Arquitetura medalhão de 3 camadas no Snowflake:

```
RAW_DATA  →  STAGING (views: conversão de tipos, renomeação, deduplicação)
          →  INTERMEDIATE (resolução de identidade, agregação de domínio)
          →  MARTS (tabelas consumidas por BI, Slack e Reverse ETL)
```

![Build do dbt](screenshots/dbt_build.png)

### Modelos de dados (13 marts)

| Modelo | Granularidade | Pergunta de negócio respondida |
|:------|:------|:--------------------------|
| `dim_accounts` | 1 linha por conta | MRR, ARR, saúde, faixa ICP e prontidão para upsell |
| `fct_accounts_health` | 1 linha por conta | Risco de churn por 3 sinais: pagamento · engajamento · suporte |
| `fct_mrr_waterfall` | conta/mês | Novo / Expansão / Redução / Churn / Reativação |
| `fct_arr_movements` | conta/mês | ARR inicial, final e tipo de variação |
| `fct_pql_signals` | 1 linha por workspace | Intenção HOT / WARM / COLD e ação GTM recomendada |
| `fct_pipeline` | 1 linha por negócio | Etapa, probabilidade de ganho e dias até o fechamento |
| `fct_subscriptions` | 1 linha por assinatura | Utilização de licenças e potencial de upsell |
| `fct_product_activation` | 1 linha por conta | Marcos de ativação e taxa de ativação |
| `fct_retention_cohorts` | coorte/mês | NRR, GRR e churn de clientes |
| `fct_trial_conversion` | 1 linha por avaliação | Conversão, tempo até converter e risco de expiração |
| `fct_unit_economics` | 1 linha por segmento | LTV e razão LTV:ARR |
| `dim_users` | 1 linha por usuário | Identidade consolidada entre sistemas |
| `dim_dates` | 1 linha por dia | Calendário para junções de séries temporais |

<details>
<summary><strong>Lógica de negócio principal</strong></summary>

**Health Score — modelo de risco com 3 sinais** (`fct_accounts_health`)

Dois ou mais sinais → `At Risk`:
```sql
is_payment_failing = (subscription_status = 'past_due')
is_churning_soon   = (cancel_at_period_end = true)
is_low_engagement  = (DATEDIFF('day', last_activity_at, CURRENT_DATE) > 30)

health_status = CASE
    WHEN subscription_status = 'canceled'                                THEN 'Churned'
    WHEN (is_payment_failing + is_churning_soon + is_low_engagement) >= 2 THEN 'At Risk'
    ELSE 'Healthy'
END
```

**Tipos de movimento na cascata de MRR** (`fct_mrr_waterfall`)

| Tipo | Definição |
|:-----|:-----------|
| `new` | Primeira assinatura neste mês |
| `expansion` | MRR aumentou em relação ao mês anterior |
| `contraction` | MRR diminuiu, mas não chegou a US$ 0 |
| `churn` | MRR → US$ 0 (cancelamento) |
| `resurrection` | MRR voltou após chegar a US$ 0 |

**Resolução de identidade** (`int_users_joined`) — fallback hierárquico:
```sql
match_method = CASE
  WHEN u.stripe_customer_id  IS NOT NULL THEN 'direct_id'
  WHEN h.hubspot_contact_id  IS NOT NULL THEN 'email_match'
  WHEN c.hubspot_company_id  IS NOT NULL THEN 'domain_l2a'
  ELSE 'unresolved'
END
```

</details>

---

## 3. Qualidade e observabilidade dos dados

**160 testes dbt** são executados a cada `dbt build`. O **Elementary** monitora anomalias entre execuções e publica falhas no Slack.

> 🛡️ **[Relatório de observabilidade ao vivo](https://vinidias.github.io/b2b-saas-revops-intelligence/elementary_report.html)**

| Camada | Quantidade | Tipos |
|:------|:------|:------|
| Testes de schema | ~130 | `unique`, `not_null`, `accepted_values` |
| Testes de relacionamentos | ~15 | Integridade de chaves estrangeiras nos marts |
| Asserções SQL customizadas | ~15 | Correção da lógica de negócio |

**SLAs de atualização das fontes:**

| Fonte | Alerta após | Erro após |
|:-------|:-----------|:-----------|
| Eventos do PostHog | 2 horas | 6 horas |
| HubSpot | 6 horas | 24 horas |
| Stripe | 12 horas | 48 horas |
| Zendesk | 24 horas | 48 horas |

![Painel do Elementary](screenshots/elementary_dashboard.png)

---

## 4. Camada semântica e agente de IA no Slack

Perguntas de negócio respondidas em linguagem natural — sem SQL, login no BI ou espera por analista.

```
"Quantas contas estão em risco esta semana?"
  → Bot do Slack → Lightdash → Snowflake
  → "14 contas — US$ 63 mil de MRR em risco 📊"
```

As métricas são definidas uma vez no YAML do dbt, disponibilizadas pelo Lightdash e consumidas pelo agente de IA no Slack.

![Demonstração do bot de IA no Slack](screenshots/slack_ai_bot_demo.gif)

**Domínios de métricas:** Core (MRR/ARR) · CS (saúde, upsell) · Finanças (cascata, NRR/GRR) · Vendas (pipeline) · Produto (PQL, ativação) · Marketing (funil, atribuição)

---

## 5. Painéis de BI (painéis como código)

O Lightdash conecta-se diretamente ao Snowflake pela camada semântica do dbt. Todos os painéis são YAML versionados.

| Painel | Público | Métricas principais |
|:----------|:---------|:------------|
| Visão executiva | CEO / diretoria | MRR total, MRR em risco, saúde das contas |
| Análise de receita para Finanças | CFO / Finanças | Cascata de MRR, NRR vs. GRR, variações de ARR |
| Saúde das contas de CS | Sucesso do Cliente | Contas em risco, motivos de churn, ações de retenção |
| Pipeline de Vendas | Liderança de Vendas / AEs | Funil, pipeline ponderado, negócios parados |
| Sinais PLG do Produto | Produto / Growth | Funil de ativação, matriz PQL, risco de avaliação |

![Painel Lightdash](screenshots/lighdash_oveview.png)

---

## 6. Reverse ETL

Insights calculados são enviados **de volta ao HubSpot** para que as equipes de GTM atuem sem sair do CRM.

| Sinal | Propriedade HubSpot | Ação GTM |
|:-------|:----------------|:-----------|
| `fct_pql_signals.intent_tier` | `pql_intent_tier` | Sequência de contato de Vendas |
| `fct_accounts_health.health_status` | `health_status` | Gatilho de ação de retenção de CS |
| `dim_accounts.is_ready_for_upsell` | `is_upsell_candidate` | Fluxo de expansão |
| `dim_accounts.arr` | `current_arr` | Contexto do negócio para Finanças e Vendas |

```
Snowflake → scripts/reverse_etl_dlt.py → API de empresas e contatos do HubSpot
```

![Registro HubSpot enriquecido](screenshots/reverse_etl_company.png)

> **Restrição:** somente contas com identidade resolvida (`match_method != 'unresolved'`) recebem enriquecimento via Reverse ETL.

→ **[Demonstração completa de Reverse ETL](REVERSE_ETL_DEMO.md)**

---

## 7. Orquestração

O Dagster executa o pipeline completo diariamente às **07:00 UTC** como DAG baseado em ativos, acompanhando a atualização dos dados e não apenas a execução de scripts.

```
07:00 UTC
  Etapa 1: Ingerir      — dlt → Snowflake RAW_DATA
  Etapa 2: Transformar  — dbt build + testes Elementary
  Etapa 3: Ativar       — Reverse ETL → HubSpot
```

```bash
dagster dev -f dagster_pipeline.py   # → http://localhost:3000
```

![Linhagem de ativos do Dagster](screenshots/dagster_full_linage.png)

---

## 8. Infraestrutura (Terraform)

A infraestrutura do data warehouse Snowflake é totalmente gerenciada como código com Terraform (provider `snowflakedb/snowflake` `~> 1.0`).

### Recursos provisionados

- **Banco de dados e schemas:** banco `REVOPS_INTELLIGENCE` com 11 schemas isolados (`RAW_DATA`, `STAGING`, `IDENTITY`, `DOMAINS`, `INTEGRATION`, `MARTS`, `SEMANTIC_LAYER`, `MARTS_CI`, `ELEMENTARY`, `MARTS_ELEMENTARY`, `MARTS_ELEMENTARY_CI`).
- **Virtual Warehouses:** mecanismos de consulta dedicados (`COMPUTE_WH`, `CI_WH`, `LOADING_WH`) com suspensão e retomada automáticas.
- **RBAC e segurança:** hierarquia de funções (`LOADER` → `TRANSFORMER` → `REPORTER`) e contas de serviço isoladas (`DBT_PROD_USER`, `DBT_CI_USER`, `DLT_LOADER_USER`, `LIGHTDASH_USER`) seguindo o princípio do menor privilégio.

```bash
cd terraform/environments/prod
cp terraform.tfvars.example terraform.tfvars  # Configure as credenciais Snowflake
terraform init
terraform plan
terraform apply
```

---

## 9. CI/CD

Cada PR aciona controles automatizados de qualidade pelo GitHub Actions.

| Workflow | Gatilho | Ação |
|:---------|:--------|:-------|
| `dbt_slim_ci.yml` | Abertura/atualização de PR | Executa no Snowflake apenas modelos alterados e seus dependentes |
| `elementary_checks.yml` | Abertura/atualização de PR | Verificações de qualidade → resultados como comentário no PR |
| `terraform_snowflake.yml` | PR / push para main | Valida, planeja e aplica IaC do Snowflake |
| `dbt_docs_deploy.yml` | Merge em `main` | Gera e publica a documentação dbt no GitHub Pages |

![Slim CI](screenshots/slim_ci.png)

> Cada merge publica automaticamente em **[vinidias.github.io/b2b-saas-revops-intelligence](https://vinidias.github.io/b2b-saas-revops-intelligence/)**

---

## Início rápido

```bash
git clone https://github.com/vinidias/b2b-saas-revops-intelligence.git
cd b2b-saas-revops-intelligence
uv venv .venv && source .venv/bin/activate
uv pip install -r requirements.txt

cp .env.example .env
# Configure: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
#            HUBSPOT_ACCESS_TOKEN, SLACK_TOKEN

# Recomendado: execute pela interface do Dagster
dagster dev -f dagster_pipeline.py   # → http://localhost:3000

# Ou execute etapa a etapa:
python ingestion/stackflow_pipeline.py   # 1. Ingestão (dados simulados de desenvolvimento)
dbt build --target snowflake             # 2. Transformação + testes
edr report --target snowflake            # 3. Relatório de observabilidade
python scripts/reverse_etl_dlt.py       # 4. Envio de sinais ao HubSpot
```

---

## Estrutura do repositório

```
b2b-saas-revops/
├── dagster_pipeline.py           # Orquestração: jobs, ativos e agenda
├── terraform/                    # Infraestrutura como código (IaC Snowflake)
│   ├── modules/                  # Módulos: banco de dados, warehouse, RBAC
│   └── environments/prod/        # Ambiente Terraform de produção
├── b2b_dlt/                      # ELT de produção — conectores de API ativos → Snowflake
│   ├── hubspot/                  # Conector HubSpot CRM
│   ├── stripe_analytics/         # Conector de faturamento Stripe
│   ├── zendesk/                  # Conector de suporte Zendesk
│   └── pg_replication/           # CDC do PostgreSQL (replicação lógica)
├── ingestion/stackflow_pipeline.py  # ELT de desenvolvimento — dados simulados → Snowflake
├── models/
│   ├── staging/                  # 8 views alinhadas às fontes
│   ├── intermediate/             # Resolução de identidade + agregação de domínio
│   └── marts/                    # 13 tabelas fact e dimensionais para o negócio
│       ├── core/  finance/  customer_success/  sales/  marketing/  product/
│       └── exposures.yml         # Linhagem dos painéis do Lightdash
├── snapshots/                    # SCD Tipo 2 (empresas HubSpot, assinaturas Stripe)
├── scripts/reverse_etl_dlt.py    # Snowflake → HubSpot (destino customizado dlt)
├── lightdash/                    # Painéis YAML como código
├── .github/workflows/            # Pipelines CI/CD (dbt, Elementary, Terraform)
└── docs/
    ├── TECHNICAL.md
    ├── DEPLOYMENT.md
    └── CASE_STUDY.md             # US$ 45 mil de ARR preservados — história completa
```

---

## Documentação relacionada

| Documento | Conteúdo |
|:----|:--------|
| [Análise técnica detalhada](docs/TECHNICAL.md) | Decisões de arquitetura, padrões de modelos e filosofia de testes |
| [Arquitetura da infraestrutura Terraform](docs/TERRAFORM_INFRASTRUCTURE.md) | Bancos Snowflake, 11 schemas, warehouses, RBAC e estado AES-256 |
| [Manual de implantação](docs/DEPLOYMENT.md) | Configuração do Snowflake, Lightdash, CI/CD e agenda do Dagster |
| [Estudo de caso](docs/CASE_STUDY.md) | US$ 45 mil de ARR preservados em 30 dias — história completa |
| [Demonstração de Reverse ETL](REVERSE_ETL_DEMO.md) | Passo a passo do pipeline ativo |
| [Demonstração de Slim CI](SLIM_CI_DEMO.md) | Demonstração passo a passo do Slim CI |

---

*Inteligência de receita de ponta a ponta. Feita para orientar decisões, não apenas painéis.*
