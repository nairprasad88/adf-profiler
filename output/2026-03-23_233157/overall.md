# Overall Rollup

## Tenant-Level Summary

| Metric | Value |
| --- | --- |
| Factory/Workspace Count | 203 |
| Pipeline Count | 499 |
| Activity Count | 785 |
| Copy Activities Defined | 147 |
| Copy Activity Runs | 181 |
| Total Data Volume (GB) | 1.31 |
| Total Pipeline Runs | 585 |
| Succeeded / Failed | 296 / 288 |
| Success Rate | 50.6% |
| SHIR Node Count | 7 |
| Database Instance Count | 58 |
| Table Reference Count | 223 |

### Cost Summary

| Cost Category | Amount |
| --- | --- |
| Total Ingestion Cost | $3.1977 |
| Orchestration Fee | $14.1510 |
| Pipeline Activity Cost | $0.1468 |
| External Activity Cost | $0.0021 |
| Dataflow Cost | $0.0000 |
| Total Orchestration Cost | $14.2999 |
| Total Estimated Cost (all) | $17.32 |
| Total Actual Cost (compute only, excl. Airflow) | $69.60 |
| Managed Airflow Actual Cost | $1234.58 |

### Ingestion Highlights

| Technique | Count |
| --- | --- |
| Full Load | 145 |
| Incremental with Watermark | 2 |

**Top 5 Source Connectors**: Files (ADLS Gen2, Blob Storage, File Storage, etc.) (66), Azure SQL Database (51), SQL Server (on-premises) (7), Databricks Delta Lake (7), REST API (3)

**Top 5 Sink Connectors**: Files (ADLS Gen2, Blob Storage, File Storage, etc.) (103), Microsoft Fabric Data Warehouse (16), Microsoft Fabric Lakehouse (13), Databricks Delta Lake (5), Azure SQL Database (5)

### Orchestration Highlights

- Datasets: 223, Linked Services: 463, Triggers: 24
- Dataflows: Gen1=25, Gen2=0
- Databricks Activities: 215
- Metadata-Driven Pipelines: Yes=1, Likely=20

## Subscription-Level Summary

| Subscription | Factories | Pipelines | Activities | Copy Acts | Pipeline Runs | Success Rate | Est. Cost |
| --- | --- | --- | --- | --- | --- | --- | --- |
| field-eng | 157 | 341 | 691 | 101 | 296 | 84.1% | $13.1668 |
| azure-test-pm | 25 | 36 | 54 | 11 | 289 | 16.3% | $4.1499 |
| **Total** | **182** | **377** | **745** | **112** | **585** | **50.6%** | **$17.3167** |

## Factory-Level Summary

| Subscription | Factory | Pipelines | Activities | Copy Acts | Runs | Success Rate | Est. Cost |
| --- | --- | --- | --- | --- | --- | --- | --- |
| field-eng | adf-sash-demo | 9 | 9 | 1 | 250 | 99.2% | $13.0565 |
| azure-test-pm | adf-adr | 3 | 6 | 0 | 45 | 97.8% | $3.3154 |
| azure-test-pm | competitive-factory | 1 | 4 | 0 | 114 | 0.0% | $0.5719 |
| azure-test-pm | adf-tanishq | 7 | 9 | 0 | 126 | 0.0% | $0.2526 |
| field-eng | bnelsondatafactory | 4 | 4 | 1 | 45 | 0.0% | $0.0906 |
| | Other (105 factories) | 475 | 753 | 145 | 5 | 80.0% | $0.0297 |
| **Total** | | **499** | **785** | **147** | **585** | | **$17.3167** |
