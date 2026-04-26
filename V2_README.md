# PVESPS-Airflow-Orchestration｜光電資料管線排程化專案

> 本專案為 **PVESPS 太陽光電發電預測與效能分析系統** 的 V2 工程化版本。  
> V1 已完成資料平台、ML training dataset、baseline model、batch scoring 與 prediction dashboard；V2 則進一步導入 **Apache Airflow**，將原本可手動執行的 Python pipeline，整理成可排程、可追蹤、可重跑的資料管線編排流程。

---

## 專案定位

PVESPS-Airflow-Orchestration 不是重新建立一個新的 ML 專案，而是把既有 PVESPS 的資料工程與機器學習流程進一步產品化。

V1 的核心重點是：

```text
資料平台 → 訓練資料 → 模型評估 → 批次推論 → 預測監控
```

V2 的核心重點則是：

```text
Airflow DAG → 任務編排 → 排程執行 → Run Log → Failure Handling → Pipeline Monitoring
```

這個版本要回答的問題是：

1. 每日資料管線如何自動排程？
2. ETL、training dataset、scoring、DQ check 之間的相依關係如何管理？
3. 任務失敗時如何追蹤、重跑與維護？
4. 如何讓 ML pipeline 更接近企業資料平台的維運模式？

---

## 為什麼加入 Airflow？

原本 PVESPS 已經具備端到端的資料與 ML workflow，但若要更接近企業資料工程情境，還需要補上「流程編排」與「排程維運」能力。

在實務環境中，資料流程通常不是單次執行，而是需要每天或定期自動執行。例如：

- 每日擷取天氣預報資料
- 更新日照與站點資料
- 產生 ML training dataset
- 執行批次預測 scoring
- 檢查資料品質與執行狀態
- 將結果提供給 dashboard 使用

Airflow 的角色，就是將這些獨立的 Python 腳本整理成一條可觀察、可維護、可重跑的 DAG pipeline。

---

## 專案亮點

- 將 PVESPS 原本的 Python ETL / ML 腳本升級為 Airflow DAG
- 使用 PostgreSQL 建立獨立資料倉儲：`pvesps_airflow_dw`
- 以 Airflow 管理每日資料管線任務相依性
- 補強資料工程作品集中的 orchestration / scheduling / observability 能力
- 展示資料管線從「可執行」到「可維運」的工程化思維

---

## 適合展示的職務方向

- Data Engineer
- Analytics Engineer
- Data Platform Engineer
- Junior Machine Learning Engineer
- ETL / ELT Pipeline Engineer
- 資料平台、資料倉儲、批次排程、AI 應用相關職缺

---

## V1 與 V2 的差異

| 版本 | 專案重點 | 主要價值 |
|---|---|---|
| PVESPS V1 | ETL + ML + Dashboard | 建立端到端太陽光電預測資料產品 |
| PVESPS V2 Airflow | Airflow + DAG + Pipeline Orchestration | 將既有流程升級為可排程、可追蹤、可維運的資料管線 |

V1 比較像是：

```text
我能建立一個完整的資料與 ML workflow。
```

V2 則是：

```text
我能把這個 workflow 編排成企業環境中可定期執行與維護的 pipeline。
```

---

## 系統架構總覽

```text
                        ┌──────────────────────────┐
                        │ Apache Airflow Webserver │
                        └─────────────┬────────────┘
                                      │
                                      ▼
                        ┌──────────────────────────┐
                        │ Airflow Scheduler        │
                        └─────────────┬────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────┐
│                    pvesps_daily_pipeline DAG                      │
└──────────────────────────────────────────────────────────────────┘
        │
        ▼
┌──────────────────────┐
│ start                │
└──────────┬───────────┘
           ▼
┌──────────────────────────────┐
│ extract_weather_forecast     │  Step_04 weather forecast ETL
└──────────┬───────────────────┘
           ▼
┌──────────────────────────────┐
│ build_training_dataset       │  Step_06 ML training dataset
└──────────┬───────────────────┘
           ▼
┌──────────────────────────────┐
│ score_generation_prediction  │  Step_08 batch scoring
└──────────┬───────────────────┘
           ▼
┌──────────────────────────────┐
│ data_quality_check           │  pipeline validation / row count check
└──────────┬───────────────────┘
           ▼
┌──────────────────────┐
│ end                  │
└──────────────────────┘
```

---

## 資料庫設計

本版本建議使用獨立資料庫：

```text
pvesps_airflow_dw
```

保留原本 PVESPS 的資料分層概念：

```text
raw      原始資料層
stg      清洗與轉換層
mart     分析、ML、prediction 使用層
meta     pipeline run log / scoring log / metadata
```

建議初始化 schema：

```sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS meta;
```

---

## 核心 DAG 設計

### DAG：`pvesps_daily_pipeline`

此 DAG 用於模擬每日光電資料管線執行流程。

#### 預計任務

| Task ID | 對應功能 | 說明 |
|---|---|---|
| `start` | pipeline start | 標記 DAG 開始 |
| `extract_weather_forecast` | Step_04 | 擷取中央氣象署天氣預報資料 |
| `build_training_dataset` | Step_06 | 建立或更新 ML training dataset |
| `score_generation_prediction` | Step_08 | 執行每日或回測模式 batch scoring |
| `data_quality_check` | DQ validation | 檢查資料列數、空值、執行狀態 |
| `end` | pipeline end | 標記 DAG 完成 |

---

## 資料流程

```text
External Data Source
  ├─ Weather Forecast API
  ├─ Sunshine Data
  └─ Solar Site Metadata

      ↓ Airflow DAG orchestration

Raw Layer
  ├─ raw_weather_forecast
  └─ raw_sunshine_daily

      ↓ ETL / cleansing / validation

Mart Layer
  ├─ mart.dim_solar_site
  ├─ mart.dim_weather_location
  ├─ mart.fact_weather_forecast
  ├─ mart.fact_sunshine_daily
  └─ mart.fact_generation_estimate

      ↓ Feature preparation

ML Layer
  └─ mart.ml_training_generation_daily

      ↓ Batch scoring

Prediction Layer
  ├─ mart.fact_generation_prediction_daily
  └─ meta.model_scoring_run_log

      ↓ Monitoring

Dashboard / Analysis Layer
  └─ prediction monitoring dashboard
```

---

## 專案結構

```text
PVESPS-Airflow-Orchestration/
├─ README.md
├─ V2_README.md
├─ .env.example
├─ requirements.txt
├─ docker-compose.airflow.yml
│
├─ dags/
│  └─ pvesps_daily_pipeline.py
│
├─ scripts/
│  ├─ run_weather_forecast_etl.py
│  ├─ run_build_training_dataset.py
│  ├─ run_score_generation_prediction.py
│  └─ run_data_quality_check.py
│
├─ sql/
│  ├─ 00_create_database.sql
│  ├─ 01_create_schemas.sql
│  └─ 02_create_meta_tables.sql
│
├─ docs/
│  ├─ airflow_architecture.md
│  ├─ dag_design.md
│  └─ operation_notes.md
│
└─ assets/
   └─ pvesps_airflow_dag.png
```

---

## 環境變數設定

請建立 `.env` 或參考 `.env.example`：

```env
# PostgreSQL for PVESPS data warehouse
PVESPS_DB_HOST=host.docker.internal
PVESPS_DB_PORT=5433
PVESPS_DB_NAME=pvesps_airflow_dw
PVESPS_DB_USER=postgres
PVESPS_DB_PASSWORD=your_password

# Airflow
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=.

# Optional API settings
CWA_API_KEY=your_cwa_api_key
```

若 Airflow 是透過 Docker container 執行，而 PostgreSQL 安裝於 Windows host，通常可先使用：

```text
host.docker.internal
```

作為 container 連線到本機資料庫的 host。

---

## 初始建置步驟

### 1. 建立資料庫

```sql
CREATE DATABASE pvesps_airflow_dw
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    CONNECTION LIMIT = -1;
```

### 2. 建立 Schema

```sql
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS meta;
```

### 3. 啟動 Airflow

```bash
docker compose -f docker-compose.airflow.yml up -d
```

### 4. 開啟 Airflow Web UI

```text
http://localhost:8080
```

### 5. 啟用 DAG

在 Airflow Web UI 中找到：

```text
pvesps_daily_pipeline
```

手動 Trigger DAG，確認 task dependency 與 log 是否正常。

---

## 第一階段開發目標

第一階段先不追求完整複雜部署，而是完成 Airflow 的最小可展示版本。

### Phase 1｜Airflow Skeleton

- 建立 Airflow Docker Compose
- 建立 `dags/` 目錄
- 建立 `pvesps_daily_pipeline.py`
- 使用 Dummy / Bash / Python task 驗證 DAG 可以正常執行
- 確認 Airflow Web UI 可以看到 DAG 與 task logs

### Phase 2｜接入 PVESPS 腳本

- 將 Step_04 ETL 包裝成 Airflow task
- 將 Step_06 training dataset 建置包裝成 Airflow task
- 將 Step_08 scoring 包裝成 Airflow task
- 加入基本 retry 與 timeout 設定

### Phase 3｜資料品質與 Run Log

- 檢查每個 task 的輸入與輸出 row count
- 寫入 `meta.etl_run_log`
- 寫入 `meta.model_scoring_run_log`
- 若資料列數異常，讓 task fail 並保留 log

### Phase 4｜作品集包裝

- 補上 DAG 架構圖
- 補上 Airflow UI 截圖
- 補上 README 操作說明
- 補上面試說明稿

---

## 預計展示的工程能力

### Data Engineering

- Batch pipeline orchestration
- ETL task dependency design
- PostgreSQL data warehouse integration
- Run logging and metadata tracking
- Data quality validation

### Airflow / Orchestration

- DAG design
- Task dependency management
- Schedule interval design
- Retry / failure handling
- Task log inspection
- Manual trigger and backfill concept

### ML Engineering

- Training dataset refresh workflow
- Batch scoring workflow
- Prediction persistence
- Model scoring run log

### Product Thinking

- 從手動腳本升級為自動化 pipeline
- 從單次模型實驗升級為定期更新流程
- 從 dashboard 展示升級為可維運資料產品

---

## 本版本暫不導入的項目

為避免作品集過度工程化，第一版 V2 暫不導入：

- CeleryExecutor
- Redis
- RabbitMQ
- Kubernetes
- Prometheus
- Grafana
- Traefik
- GitLab CI/CD

這些項目可作為後續強化方向，但初版重點會放在：

```text
Airflow DAG + PostgreSQL + PVESPS pipeline scripts
```

---

## 後續優化方向

### Airflow 面

- 加入 scheduled daily run
- 加入 backfill 說明與範例
- 加入 task-level retry policy
- 加入 SLA / timeout 設定
- 將 DAG 拆分為 ETL DAG 與 ML Scoring DAG

### 資料面

- 加入更完整的 row count validation
- 加入資料日期 coverage check
- 加入 forecast issue time freshness check
- 加入 scoring output completeness check

### 產品面

- Dashboard 顯示 latest successful DAG run
- Dashboard 顯示 last scoring date
- Dashboard 顯示 pipeline health status
- 加入異常站點與 pipeline 狀態摘要

### DevOps 面

- 建立 GitHub Actions 基本檢查
- 加入 requirements lock
- 補上 Docker build 說明
- 補上 local development guide

---

## 面試說明稿

可以用以下方式介紹本專案：

> PVESPS V1 已完成太陽光電資料平台、ML training dataset、模型訓練、batch scoring 與 prediction dashboard。  
> V2 則進一步導入 Apache Airflow，將原本獨立執行的 Python 腳本整理成可排程的 DAG。  
> 透過 DAG 管理天氣資料擷取、訓練資料更新、批次預測與資料品質檢查，讓整個流程更接近企業資料平台的每日批次維運模式。  
> 這個版本的重點不是再增加模型複雜度，而是展示我能把資料與 ML workflow 轉換成可維護、可追蹤、可重跑的 production-like pipeline。

---

## 商業意義

V1 展示的是太陽光電資料如何從原始資料轉換為預測與監控結果。

V2 補上的則是營運層面的穩定性：

- 資料每天是否有更新？
- pipeline 哪一步失敗？
- 預測是否有成功產生？
- dashboard 讀到的是不是最新資料？
- 異常時是否能快速追蹤與重跑？

這讓 PVESPS 不只是單一作品集，而更像一個可持續維運的資料產品雛形。

---

## 最終總結

PVESPS-Airflow-Orchestration 是 PVESPS 的工程化升級版本。

它展示的重點不是「多做一個工具」，而是把既有的資料與 ML 流程提升到更接近職場的資料管線維運層級。

本專案預期完成後，將具備：

- Airflow DAG 編排
- PostgreSQL data warehouse 串接
- ETL / training dataset / scoring task dependency
- pipeline run log 與資料品質檢查
- 可展示的 orchestration architecture
- 可用於面試說明的 production-like data pipeline 敘事

一句話總結：

> V1 證明我能打通資料與 ML 流程；V2 證明我知道如何讓這條流程穩定地每天執行、追蹤與維護。
