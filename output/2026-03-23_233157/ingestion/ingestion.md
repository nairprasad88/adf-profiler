# Ingestion Rollup

## 1. Ingestion Overview (Tenant Level)

| Metric | Value |
| --- | --- |
| Total Copy Activities Defined | 147 |
| Total Copy Activity Runs | 181 |
| Total Data Read (GB) | 0.66 |
| Total Data Written (GB) | 0.66 |
| Total Data Volume (GB) | 1.31 |
| Total Ingestion Cost (estimated) | $3.1977 |
| Runs Per Day Average | 2.01 |
| Profiling Window | 90 days |

## 2. Connectors

| Connector | As Source | As Sink | Total |
| --- | --- | --- | --- |
| Files (ADLS Gen2, Blob Storage, File Storage, etc.) | 66 | 103 | 169 |
| Azure SQL Database | 51 | 5 | 56 |
| Microsoft Fabric Data Warehouse | 0 | 16 | 16 |
| Microsoft Fabric Lakehouse | 3 | 13 | 16 |
| Databricks Delta Lake | 7 | 5 | 12 |
| SQL Server (on-premises) | 7 | 0 | 7 |
| ODBC | 2 | 1 | 3 |
| REST API | 3 | 0 | 3 |
| Azure Cosmos DB | 1 | 1 | 2 |
| Azure Synapse Analytics (SQL Pool) | 0 | 2 | 2 |
| Blob | 1 | 1 | 2 |
| HTTP | 2 | 0 | 2 |
| SFTP | 2 | 0 | 2 |
| Salesforce | 1 | 0 | 1 |
| Snowflake | 1 | 0 | 1 |
| **Total** | **147** | **147** | **294** |

## 3. Ingestion Techniques

| Technique | Count | % of Total |
| --- | --- | --- |
| Full Load | 145 | 98.6% |
| Incremental with Watermark | 2 | 1.4% |

### Technique Detection Methodology

Each copy activity is classified based on the pipeline structure around it:

- **Full Load**: Copy activity with no downstream transformation activity in the same pipeline.
- **Full Load + Stored Procedure**: Copy activity followed by a SqlServerStoredProcedure activity downstream (directly or transitively).
- **Full Load + Databricks / Synapse / HDInsight**: Copy activity followed by a downstream activity whose type indicates a specific vendor (e.g., DatabricksNotebook, SynapseNotebook, HDInsightHive). The vendor name is derived from the downstream activity type or linked service type.
- **Incremental with Watermark**: A Lookup activity upstream of the copy that uses parameter naming conventions suggestive of watermark tracking (e.g., watermark, lastModified, incrementalStart).
- **Copy Upsert (built-in)**: The copy activity sink has writeBehavior set to 'upsert' or uses a merge/upsert option.
- **Native CDC**: Detection is pattern-based on SQL query text containing CDC-related keywords (e.g., cdc.fn_cdc_get_all_changes, __$operation, CHANGE_TRACKING).

### Assumptions and Limitations

- Stored procedure contents cannot be inspected -- we only know a stored proc runs after copy. Whether it performs MERGE, UPSERT, or other logic is unknown.
- Databricks notebook contents cannot be inspected -- only the notebook path is available.
- CDC detection is pattern-based on SQL query text found in the pipeline definition. Actual database-level CDC enablement cannot be verified.
- Watermark detection checks for Lookup activities upstream of copy with parameter naming conventions. False positives are possible if parameter names happen to match.
- Classification is heuristic and should be validated.

## 4. Breakdown by Subscription

| Subscription | Factories | Total IRs | Azure IRs | SHIRs | SSIS IRs | Managed VNet IRs | Copy Activities | Copy Runs | Data Volume (GB) | Est. Cost |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| field-eng | 157 | 36 | 18 | 18 | 0 | 0 | 101 | 1 | 0.00 | $0.0177 |
| azure-test-pm | 25 | 1 | 0 | 1 | 0 | 0 | 11 | 180 | 1.31 | $3.1801 |
| **Total** | **182** | **37** | **18** | **19** | **0** | **0** | **112** | **181** | **1.31** | **$3.1977** |

## 5. Breakdown by Factory

| Subscription | Factory | Total IRs | Azure IRs | SHIRs | SSIS IRs | Managed VNet IRs | Copy Activities | Copy Runs | Data Volume (GB) | Est. Cost |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| azure-test-pm | adf-adr | 0 | 0 | 0 | 0 | 0 | 0 | 180 | 1.31 | $3.1801 |
| field-eng | steven-psa-orchestration | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0.00 | $0.0177 |
| field-eng | laf-adf | 3 | 1 | 2 | 0 | 0 | 57 | 0 | 0.00 | $0.0000 |
| field-eng | fieldeng-migrations | 1 | 1 | 0 | 0 | 0 | 1 | 0 | 0.00 | $0.0000 |
| field-eng | bnelsondatafactory | 1 | 1 | 0 | 0 | 0 | 1 | 0 | 0.00 | $0.0000 |
| | Other (29 factories) | 6 | 3 | 3 | 0 | 0 | 88 | 0 | 0.00 | $0.0000 |
| **Total** | | **11** | **6** | **5** | **0** | **0** | **147** | **181** | **1.31** | **$3.1977** |

## 6. Integration Runtime Breakdown

### Tenant Level

| IR Type | Defined | Activity Runs | Total DIU-Hours | Est. Cost |
| --- | --- | --- | --- | --- |
| Azure IR | 18 | 181 | 2.50 | $3.1977 |
| Self-Hosted IR | 19 | 0 | 0.00 | $0.0000 |
| **Total** | **37** | **181** | **2.50** | **$3.1977** |

### By Subscription

| Subscription | Azure IRs | SHIRs | SSIS IRs | Managed VNet IRs | Activity Runs | DIU-Hours | Est. Cost |
| --- | --- | --- | --- | --- | --- | --- | --- |
| field-eng | 18 | 18 | 0 | 0 | 1 | 0.01 | $0.0177 |
| azure-test-pm | 0 | 1 | 0 | 0 | 180 | 2.49 | $3.1801 |
| **Total** | **18** | **19** | **0** | **0** | **181** | **2.50** | **$3.1977** |

### Azure IRs

| IR Name | Factory | State | Activity Runs | DIU-Hours | Data Volume (GB) | Est. Cost |
| --- | --- | --- | --- | --- | --- | --- |
| AutoResolveIntegrationRuntime * | adf-adr | Auto-Provisioned | 180 | 2.49 | 1.31 | $3.1801 |
| AutoResolveIntegrationRuntime * | steven-psa-orchestration | Auto-Provisioned | 1 | 0.01 | 0.00 | $0.0177 |
| integrationRuntime-SSC | ssc-adf-priv | N/A | 0 | 0.00 | 0.00 | $0.0000 |
| integrationRuntime1 | fieldeng-migrations | N/A | 0 | 0.00 | 0.00 | $0.0000 |
| AutoResolveIntegrationRuntime | jl-adf-test | N/A | 0 | 0.00 | 0.00 | $0.0000 |
| Other (15 Azure IRs) | | | 0 | 0.00 | 0.00 | $0.0000 |
| **Total (20)** | | | **181** | **2.50** | **1.31** | **$3.1977** |

*\* Auto-provisioned IRs were not explicitly defined in the factory but were used at runtime (e.g., AutoResolveIntegrationRuntime).*

### Self-Hosted IRs

| IR Name | Factory | Nodes | Cores | Memory (GB) | Last Connected | Activity Runs | Data Volume (GB) | Est. Cost |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| stuartADFSQLlink | stuart-adf | 1 | Unknown | 4.1 | 2019-11-13 | 0 | 0.00 | $0.0000 |
| integrationRuntime1 | fe-shared-ssa-latam-adf-001 | N/A | N/A | N/A | N/A | 0 | 0.00 | $0.0000 |
| SAPHANADbrxintegrationRuntime | SAP-HANA-DBRX-ADF2 | N/A | N/A | N/A | N/A | 0 | 0.00 | $0.0000 |
| integrationRuntime1 | ganeshrj-df-demo | 1 | Unknown | 12.7 | 2022-09-26 | 0 | 0.00 | $0.0000 |
| integrationRuntime4 | ganeshrj-df-demo | N/A | N/A | N/A | N/A | 0 | 0.00 | $0.0000 |
| Other (14 SHIRs) | | | | | | 0 | 0.00 | $0.0000 |
| **Total (19)** | | | | | | **0** | **0.00** | **$0.0000** |

#### SHIR Node Details

| Node Name | Factory | IR Name | Status | Cores | Memory (GB) | Last Connected |
| --- | --- | --- | --- | --- | --- | --- |
| fe-cis-imrans-p | fe-cis-imrans-adf | ire-1 | Offline | Unknown | 3.8 | 2026-03-19 |
| laf-vm-pltest | laf-adf | integrationRuntime1 | Offline | Unknown | 28.3 | 2024-09-24 |
| EC2AMAZ-D9VIOKG | peteradfcuj | peterintegrationruntime | Offline | Unknown | 25.3 | 2026-03-11 |
| WINDOWS-JDA4569 | laf-adf | integrationRuntime2 | Offline | Unknown | 0.5 | 2025-07-24 |
| nshetty-perform | nshetty-test | nshetty-performance-ir | Offline | 8 | 32.0 | 2026-02-02 |
| ganeshrje2vm | ganeshrj-df-demo | integrationRuntime1 | Offline | Unknown | 12.7 | 2022-09-26 |
| stuartsql | stuart-adf | stuartADFSQLlink | Offline | Unknown | 4.1 | 2019-11-13 |

*Note: Est. Cost reflects only ADF data movement charges for copy activities through this SHIR. It does not include VM infrastructure costs.*


## 7. Database & Table Discovery

| Metric | Value |
| --- | --- |
| Database Instance Count | 58 |
| Table Reference Count | 223 |

### Limitations

- **Tables**: Only finds tables referenced in datasets linked to pipelines. Misses tables referenced in stored procedures, Databricks notebooks, dynamic SQL, parameterized queries, and data flow transformations that use inline datasets.
- **Database instances**: Only those with explicit linked services in ADF. Databases accessed only via Databricks, stored procedure internal connections, or connection strings embedded in code are not discovered.
- The counts represent a **lower bound** of actual database/table usage.
