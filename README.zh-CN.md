# B2B SaaS RevOps 智能分析引擎

> **[🌐 在线门户](https://vinidias.github.io/b2b-saas-revops-intelligence/)** · **[📚 dbt 文档](https://vinidias.github.io/b2b-saas-revops-intelligence/dbt_docs/)** · **[🛡️ 可观测性报告](https://vinidias.github.io/b2b-saas-revops-intelligence/elementary_report.html)**

**语言：** [English](README.md) · [Português (Brasil)](README.pt-BR.md) · [简体中文](README.zh-CN.md)

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

## 目录

1. [项目简介](#项目简介)
2. [业务背景](#业务背景)
3. [架构](#架构)
4. [数据摄取（EL）](#1-数据摄取el)
5. [数据转换（dbt）](#2-数据转换dbt)
6. [数据质量与可观测性](#3-数据质量与可观测性)
7. [语义层与 Slack AI 智能体](#4-语义层与-slack-ai-智能体)
8. [BI 仪表板](#5-bi-仪表板代码化仪表板)
9. [反向 ETL](#6-反向-etl)
10. [编排](#7-编排)
11. [基础设施（Terraform）](#8-基础设施terraform)
12. [CI/CD](#9-cicd)
13. [快速开始](#快速开始)
14. [仓库结构](#仓库结构)
15. [相关文档](#相关文档)

---

## 项目简介

这是一个可用于生产环境的 **Revenue Operations（收入运营）数据管道**，将 CRM、计费、支持和产品数据整合为统一可信的数据源，并自动向 GTM 团队提供可执行的洞察。

| | |
|:-|:-|
| **问题** | 4 个彼此孤立的工具（HubSpot · Stripe · Zendesk · PostHog），身份无法关联、流失风险隐蔽、MRR 不可靠 |
| **方案** | 端到端管道：摄取 → 转换 → 语义层 → Slack AI 智能体 → 反向 ETL 回写 CRM |
| **结果** | 首次运行发现 **23 个高风险客户（ARR 8.7 万美元）**。客户成功团队在 30 天内保住了 **4.5 万美元 ARR** |
| **技术栈** | dlt · dbt · Snowflake · Dagster · Lightdash · Elementary · Slack Bot |
| **基础设施成本** | 每月 0 美元 — Snowflake 免费额度及开源工具 |

---

## 业务背景

**[StackFlow AI](https://stackflow.ai)** — B2B SaaS 工程管理平台。ARR 为 300 万美元，A 轮融资 1,000 万美元，员工超过 60 人。高速增长暴露出结构性问题：各团队使用各自优秀的工具，却彼此不通。

| 团队 | 信息盲区 |
|:-----|:-----------|
| 销售（HubSpot） | 无法确认赢得的商机是否真的在产品中完成激活 |
| 财务（Stripe） | 无法看到每笔订阅来自哪个营销活动或销售代表 |
| 客户成功（Zendesk） | 不清楚高风险客户的产品参与度 |
| 产品（PostHog） | 无法量化低参与度客户带来的 ARR 风险 |

**实际损失：**
- **隐性流失** — 一个年费 1.2 万美元的 Enterprise 客户，在 Stripe 逾期 3 周、Git 活动中断 6 周且有 4 个未解决的严重工单后取消。每个团队都看到一个信号，却没有人看到全部三个。
- **错失扩张机会** — 30% 的客户席位使用率超过 80%，销售团队却仍在冷启动拓客。
- **MRR 不准确** — 财务团队每月手工制作董事会报告，耗时 16 小时，总是滞后 2–3 周，误差达 ±8%。
- **分析师瓶颈** — 一个简单的客户细分问题也要 3 天才能回答。数据并非不存在，而是分散在 4 个系统中。

---

## 架构

```
HubSpot · Stripe · Zendesk · PostHog · 内部数据库
            ↓  （dlt — 增量加载，自动推断 schema）
         Snowflake RAW_DATA
            ↓  （dbt — 三层奖章架构）
    STAGING → INTERMEDIATE → MARTS
            ↓                      ↓
    Lightdash + Slack AI      反向 ETL → HubSpot
            ↓
       Dagster（每日 07:00 UTC 编排）
```

![完整数据架构](screenshots/full_data_architecture.png)

<details>
<summary><strong>完整技术栈</strong></summary>

| 层 | 工具 | 职责 |
|:------|:-----|:-----|
| 数据摄取 | dlt | 5 个实时连接器 → Snowflake；增量加载、自动推断 schema |
| 数据转换 | dbt | 基于 DAG 的 SQL，包含测试、文档和血缘关系 |
| 数据仓库 | Snowflake | 云数据仓库 |
| 可观测性 | Elementary | 异常检测与 Slack 告警 |
| 语义层 | Lightdash | 从 dbt `meta` YAML 定义指标即代码 |
| AI 问答 | Lightdash AI + Slack Bot | 自然语言 → Snowflake；无需编写 SQL |
| 编排 | Dagster | 基于资产的 DAG，跟踪数据新鲜度而非仅跟踪脚本运行 |
| 反向 ETL | Python + dlt | 将洞察回写到 HubSpot |
| CI/CD | GitHub Actions | Slim CI、Elementary 检查、自动发布 dbt 文档 |

</details>

---

## 1. 数据摄取（EL）

**dlt** 将 5 个数据源摄取到 Snowflake 的 `RAW_DATA`，支持自动 schema 推断、增量加载和分页。

| 数据源 | 方法 | 主要表 |
|:-------|:-------|:-----------|
| HubSpot CRM | REST API + 属性历史 | `companies`、`contacts`、`deals` |
| Stripe Billing | REST API（基于 cursor） | `subscriptions`、`invoices`、`customers` |
| Zendesk Support | REST API（增量） | `tickets`、`users`、`organizations` |
| PostHog 事件 | REST API | `events`、`persons` |
| 内部数据库 | PostgreSQL CDC（逻辑复制） | `users`、`workspaces`、`seats` |

**开发模式：** `ingestion/stackflow_pipeline.py` — 模拟数据 → `DEV_RAW_DATA`  
**生产模式：** [`b2b_dlt/`](b2b_dlt/) — 实时 API 连接器 → `RAW_DATA`

![dlt 摄取输出](screenshots/dlt_load_snowflake_terminal.png)

---

## 2. 数据转换（dbt）

Snowflake 上的三层奖章架构：

```
RAW_DATA  →  STAGING（视图：类型转换、重命名、去重）
          →  INTERMEDIATE（身份匹配、领域聚合）
          →  MARTS（供 BI、Slack 和反向 ETL 使用的表）
```

![dbt 构建](screenshots/dbt_build.png)

### 数据模型（13 个 marts）

| 模型 | 粒度 | 回答的业务问题 |
|:------|:------|:--------------------------|
| `dim_accounts` | 每个客户 1 行 | MRR、ARR、健康状态、ICP 层级、追加销售准备度 |
| `fct_accounts_health` | 每个客户 1 行 | 三类流失风险信号：付款 · 参与度 · 支持 |
| `fct_mrr_waterfall` | 客户/月 | 新增 / 扩张 / 收缩 / 流失 / 恢复 |
| `fct_arr_movements` | 客户/月 | ARR 起始值、结束值和变动类型 |
| `fct_pql_signals` | 每个 workspace 1 行 | HOT / WARM / COLD 意向评分及建议的 GTM 行动 |
| `fct_pipeline` | 每笔商机 1 行 | 商机阶段、赢单概率、预计成交天数 |
| `fct_subscriptions` | 每个订阅 1 行 | 席位使用率、追加销售候选 |
| `fct_product_activation` | 每个客户 1 行 | 激活里程碑和激活率 |
| `fct_retention_cohorts` | cohort/月 | NRR、GRR、客户流失率 |
| `fct_trial_conversion` | 每次试用 1 行 | 转化状态、转化时间、到期风险 |
| `fct_unit_economics` | 每个细分 1 行 | LTV、LTV:ARR 比率 |
| `dim_users` | 每个用户 1 行 | 跨系统整合后的用户身份 |
| `dim_dates` | 每天 1 行 | 时间序列关联的日期维表 |

<details>
<summary><strong>核心业务逻辑</strong></summary>

**健康评分 — 三信号风险模型**（`fct_accounts_health`）

任意 2 个或更多信号 → `At Risk`：
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

**MRR 瀑布变动类型**（`fct_mrr_waterfall`）

| 类型 | 定义 |
|:-----|:-----------|
| `new` | 本月首次订阅 |
| `expansion` | MRR 相较上月增加 |
| `contraction` | MRR 减少但不为 0 |
| `churn` | MRR → 0（取消） |
| `resurrection` | MRR 从 0 恢复 |

**身份解析**（`int_users_joined`）— 分层回退策略：
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

## 3. 数据质量与可观测性

每次 `dbt build` 都会运行 **160 项 dbt 测试**。**Elementary** 监控运行之间的异常，并将失败通知发送到 Slack。

> 🛡️ **[在线可观测性报告](https://vinidias.github.io/b2b-saas-revops-intelligence/elementary_report.html)**

| 层 | 数量 | 类型 |
|:------|:------|:------|
| Schema 测试 | ~130 | `unique`、`not_null`、`accepted_values` |
| 关系测试 | ~15 | marts 中的外键完整性 |
| 自定义 SQL 断言 | ~15 | 业务逻辑正确性 |

**数据源新鲜度 SLA：**

| 数据源 | 告警阈值 | 错误阈值 |
|:-------|:-----------|:-----------|
| PostHog 事件 | 2 小时 | 6 小时 |
| HubSpot | 6 小时 | 24 小时 |
| Stripe | 12 小时 | 48 小时 |
| Zendesk | 24 小时 | 48 小时 |

![Elementary 仪表板](screenshots/elementary_dashboard.png)

---

## 4. 语义层与 Slack AI 智能体

使用自然语言回答业务问题 — 无需 SQL、无需登录 BI，也无需等待分析师。

```
“本周有多少个高风险客户？”
  → Slack Bot → Lightdash → Snowflake
  → “14 个客户 — 6.3 万美元 MRR 面临风险 📊”
```

指标只需在 dbt YAML 中定义一次，即可由 Lightdash 提供并由 Slack AI Bot 使用。

![Slack AI Bot 演示](screenshots/slack_ai_bot_demo.gif)

**指标领域：** Core（MRR/ARR） · CS（健康度、追加销售） · 财务（瀑布、NRR/GRR） · 销售（pipeline） · 产品（PQL、激活） · 营销（漏斗、归因）

---

## 5. BI 仪表板（仪表板即代码）

Lightdash 通过 dbt 语义层直接连接 Snowflake。所有仪表板都以 YAML 形式进行版本控制。

| 仪表板 | 使用者 | 关键指标 |
|:----------|:---------|:------------|
| 高管总览 | CEO / 高管团队 | 总 MRR、风险 MRR、客户健康度 |
| 财务收入分析 | CFO / 财务 | MRR 瀑布、NRR 与 GRR、ARR 变动 |
| CS 客户健康度 | 客户成功 | 高风险客户列表、流失原因、挽留方案 |
| 销售 Pipeline | 销售负责人 / AE | 商机漏斗、加权 Pipeline、停滞商机 |
| 产品 PLG 信号 | 产品 / 增长 | 激活漏斗、PQL 矩阵、试用风险 |

![Lightdash 仪表板](screenshots/lighdash_oveview.png)

---

## 6. 反向 ETL

将计算出的洞察**回写至 HubSpot**，让 GTM 团队无需离开 CRM 就能采取行动。

| 信号 | HubSpot 属性 | GTM 行动 |
|:-------|:----------------|:-----------|
| `fct_pql_signals.intent_tier` | `pql_intent_tier` | 销售触达序列 |
| `fct_accounts_health.health_status` | `health_status` | 触发 CS 挽留方案 |
| `dim_accounts.is_ready_for_upsell` | `is_upsell_candidate` | 扩张工作流 |
| `dim_accounts.arr` | `current_arr` | 为财务和销售提供商机背景 |

```
Snowflake → scripts/reverse_etl_dlt.py → HubSpot Companies & Contacts API
```

![HubSpot 丰富化记录](screenshots/reverse_etl_company.png)

> **限制：** 仅为身份已解析的客户（`match_method != 'unresolved'`）执行反向 ETL 丰富化。

→ **[反向 ETL 完整演示](REVERSE_ETL_DEMO.md)**

---

## 7. 编排

Dagster 每天 **07:00 UTC** 以资产 DAG 的方式运行完整管道，跟踪数据新鲜度，而不仅仅是脚本执行情况。

```
07:00 UTC
  步骤 1：摄取   — dlt → Snowflake RAW_DATA
  步骤 2：转换   — dbt build + Elementary 测试
  步骤 3：激活   — 反向 ETL → HubSpot
```

```bash
dagster dev -f dagster_pipeline.py   # → http://localhost:3000
```

![Dagster 资产血缘](screenshots/dagster_full_linage.png)

---

## 8. 基础设施（Terraform）

Snowflake 数据仓库基础设施完全通过 Terraform 以代码方式管理（provider `snowflakedb/snowflake` `~> 1.0`）。

### 已配置资源

- **数据库与 schemas：** `REVOPS_INTELLIGENCE` 数据库，包含 11 个隔离的 schemas（`RAW_DATA`、`STAGING`、`IDENTITY`、`DOMAINS`、`INTEGRATION`、`MARTS`、`SEMANTIC_LAYER`、`MARTS_CI`、`ELEMENTARY`、`MARTS_ELEMENTARY`、`MARTS_ELEMENTARY_CI`）。
- **Virtual Warehouses：** 专用查询引擎（`COMPUTE_WH`、`CI_WH`、`LOADING_WH`），支持自动挂起和自动恢复。
- **RBAC 与安全：** 角色层级（`LOADER` → `TRANSFORMER` → `REPORTER`）和隔离的服务账户（`DBT_PROD_USER`、`DBT_CI_USER`、`DLT_LOADER_USER`、`LIGHTDASH_USER`），遵循最小权限原则。

```bash
cd terraform/environments/prod
cp terraform.tfvars.example terraform.tfvars  # 设置 Snowflake 凭据
terraform init
terraform plan
terraform apply
```

---

## 9. CI/CD

每个 PR 都会通过 GitHub Actions 触发自动化质量门禁。

| Workflow | 触发条件 | 操作 |
|:---------|:--------|:-------|
| `dbt_slim_ci.yml` | PR 创建/更新 | 在 Snowflake 上仅构建变更模型及其下游模型 |
| `elementary_checks.yml` | PR 创建/更新 | 数据质量检查 → 将结果作为 PR 评论发布 |
| `terraform_snowflake.yml` | PR / 推送至 main | 验证、计划并应用 Snowflake IaC |
| `dbt_docs_deploy.yml` | 合并至 `main` | 生成并部署 dbt 文档到 GitHub Pages |

![Slim CI](screenshots/slim_ci.png)

> 每次合并都会自动部署到 **[vinidias.github.io/b2b-saas-revops-intelligence](https://vinidias.github.io/b2b-saas-revops-intelligence/)**

---

## 快速开始

```bash
git clone https://github.com/vinidias/b2b-saas-revops-intelligence.git
cd b2b-saas-revops-intelligence
uv venv .venv && source .venv/bin/activate
uv pip install -r requirements.txt

cp .env.example .env
# 配置：SNOWFLAKE_ACCOUNT、SNOWFLAKE_USER、SNOWFLAKE_PASSWORD，
#       HUBSPOT_ACCESS_TOKEN、SLACK_TOKEN

# 推荐：通过 Dagster UI 运行
dagster dev -f dagster_pipeline.py   # → http://localhost:3000

# 或逐步运行：
python ingestion/stackflow_pipeline.py   # 1. 摄取（开发模拟数据）
dbt build --target snowflake             # 2. 转换 + 测试
edr report --target snowflake            # 3. 可观测性报告
python scripts/reverse_etl_dlt.py       # 4. 将信号回写到 HubSpot
```

---

## 仓库结构

```
b2b-saas-revops/
├── dagster_pipeline.py           # 编排：jobs、assets、schedule
├── terraform/                    # 基础设施即代码（Snowflake IaC）
│   ├── modules/                  # 模块：数据库、warehouse、RBAC
│   └── environments/prod/        # Terraform 生产环境
├── b2b_dlt/                      # 生产 ELT — 实时 API 连接器 → Snowflake
│   ├── hubspot/                  # HubSpot CRM 连接器
│   ├── stripe_analytics/         # Stripe 计费连接器
│   ├── zendesk/                  # Zendesk 支持连接器
│   └── pg_replication/           # PostgreSQL CDC（逻辑复制）
├── ingestion/stackflow_pipeline.py  # 开发 ELT — 模拟数据 → Snowflake
├── models/
│   ├── staging/                  # 8 个按数据源组织的 views
│   ├── intermediate/             # 身份解析 + 领域聚合
│   └── marts/                    # 13 个业务 fact 和 dimension 表
│       ├── core/  finance/  customer_success/  sales/  marketing/  product/
│       └── exposures.yml         # Lightdash 仪表板血缘
├── snapshots/                    # SCD Type 2（HubSpot companies、Stripe subscriptions）
├── scripts/reverse_etl_dlt.py    # Snowflake → HubSpot（dlt 自定义目标）
├── lightdash/                    # 仪表板即代码 YAML
├── .github/workflows/            # CI/CD 流水线（dbt、Elementary、Terraform）
└── docs/
    ├── TECHNICAL.md
    ├── DEPLOYMENT.md
    └── CASE_STUDY.md             # 30 天保住 4.5 万美元 ARR — 完整案例
```

---

## 相关文档

| 文档 | 内容 |
|:----|:--------|
| [技术深入解析](docs/TECHNICAL.md) | 架构决策、模型模式和测试理念 |
| [Terraform 基础设施架构](docs/TERRAFORM_INFRASTRUCTURE.md) | Snowflake 数据库、11 个 schemas、warehouses、RBAC、AES-256 状态 |
| [部署运行手册](docs/DEPLOYMENT.md) | Snowflake、Lightdash、CI/CD 和 Dagster 调度配置 |
| [案例研究](docs/CASE_STUDY.md) | 30 天保住 4.5 万美元 ARR — 完整案例 |
| [反向 ETL 演示](REVERSE_ETL_DEMO.md) | 实时管道逐步演示 |
| [Slim CI 演示](SLIM_CI_DEMO.md) | Slim CI 逐步演示 |

---

*端到端收入智能。为推动决策而构建，而不只是展示仪表板。*
