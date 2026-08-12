# PVESPS AirFlow Orchestration

以 PostgreSQL、CWA 天氣預報、機器學習與 Apache Airflow 建立的太陽能發電預測與資料編排作品集專案。

## 專案內容

- `Step_01_建立資料庫`：PostgreSQL schema 與初始化 SQL
- `Step_04_取得最近三日預報API`：CWA 天氣預報 ETL 與天氣特徵解析
- `Step_06_建立ML訓練資料集`、`Step_07_訓練基準模型與樹模型`：訓練資料、基準模型與 Random Forest
- `Step_08_批次推論與結果落庫`：day-ahead 預測、結果落庫與品質欄位
- `Step_11_airflow_排程管理`：Airflow DAG、Docker Compose 與可重跑任務腳本
- `assets/`：Power BI P1–P6 與系統架構示意圖

## 安全與執行說明

本公開版本不包含 `.env`、API key、Airflow 密碼檔、Power BI `.pbix` 與 `gitignore_BOX/` 備份。請依各步驟文件建立本機環境變數後再執行；Airflow Compose 中的 `AIRFLOW_JWT_SECRET` 也必須使用本機 `.env` 設定。

各 Step 資料夾內提供對應的 README、操作說明與 SQL。