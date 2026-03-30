# Orchestration Rollup

## 1. Orchestration Overview (Tenant Level)

| Metric | Value |
| --- | --- |
| Total Pipelines | 499 |
| Total Activities | 785 |
| Total Pipeline Runs | 585 |
| Succeeded Runs | 296 |
| Failed Runs | 288 |
| Success Rate | 50.6% |
| Avg Pipeline Duration (min) | 1.51 |

### Cost Breakdown

| Cost Category | Amount |
| --- | --- |
| Orchestration fees (per activity run) | $14.1510 |
| Pipeline activity cost (Lookup, GetMetadata, etc.) | $0.1468 |
| External activity cost (stored procs, web activities, Databricks) | $0.0021 |
| Dataflow cost ($0.274/vCore-hr x 8 vCores min) | $0.0000 |
| Total Orchestration Cost | $14.2999 |

## 2. Activity Type Distribution

### Control Flow Activities

| Activity Type | Count |
| --- | --- |
| SetVariable | 57 |
| Until | 46 |
| ForEach | 35 |
| IfCondition | 29 |
| ExecutePipeline | 23 |
| Wait | 12 |
| AppendVariable | 4 |
| Switch | 2 |
| Filter | 1 |
| Validation | 1 |
| **Subtotal** | **210** |

### Data Movement Activities

| Activity Type | Count |
| --- | --- |
| Copy | 147 |
| **Subtotal** | **147** |

### Transformation Activities

| Activity Type | Count |
| --- | --- |
| DatabricksNotebook | 212 |
| ExecuteDataFlow | 17 |
| DatabricksSparkPython | 2 |
| DatabricksSparkJar | 1 |
| **Subtotal** | **232** |

### External Activities

| Activity Type | Count |
| --- | --- |
| WebActivity | 111 |
| Lookup | 26 |
| SqlServerStoredProcedure | 16 |
| GetMetadata | 5 |
| Delete | 4 |
| **Subtotal** | **162** |

### Other Activities

| Activity Type | Count |
| --- | --- |
| Unknown | 32 |
| TridentNotebook | 2 |
| **Subtotal** | **34** |

**Total Activities**: 785

## 3. Breakdown by Subscription

| Subscription | Pipelines | Activities | Runs | Success Rate | Orch | Pipeline Act | External | Dataflow | Total Cost |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| field-eng | 341 | 691 | 296 | 84.1% | $13.0040 | $0.1455 | $0.0006 | $0.0000 | $13.1501 |
| azure-test-pm | 36 | 54 | 289 | 16.3% | $1.1470 | $0.0013 | $0.0015 | $0.0000 | $1.1498 |
| **Total** | **377** | **745** | **585** | **50.6%** | **$14.1510** | **$0.1468** | **$0.0021** | **$0.0000** | **$14.2999** |

## 4. Breakdown by Factory

| Subscription | Factory | Pipelines | Runs | Success Rate | Orch | Pipeline Act | External | Dataflow | Total Cost |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| field-eng | adf-sash-demo | 1 | 250 | 99.2% | $12.9110 | $0.1455 | $0.0000 | $0.0000 | $13.0565 |
| azure-test-pm | competitive-factory | 1 | 114 | 0.0% | $0.5700 | $0.0009 | $0.0009 | $0.0000 | $0.5718 |
| azure-test-pm | adf-adr | 1 | 45 | 97.8% | $0.3150 | $0.0004 | $0.0000 | $0.0000 | $0.3154 |
| azure-test-pm | adf-tanishq | 3 | 126 | 0.0% | $0.2520 | $0.0000 | $0.0006 | $0.0000 | $0.2526 |
| field-eng | bnelsondatafactory | 1 | 45 | 0.0% | $0.0900 | $0.0000 | $0.0006 | $0.0000 | $0.0906 |
| | Other (2 factories) | 4 | 5 | 80.0% | $0.0130 | $0.0000 | $0.0000 | $0.0000 | $0.0130 |
| **Total** | | **11** | **585** | | **$14.1510** | **$0.1468** | **$0.0021** | **$0.0000** | **$14.2999** |

## 5. Databricks Usage

### Interactive Activities

| Activity Type | Count |
| --- | --- |
| DatabricksNotebook | 212 |
| DatabricksSparkPython | 2 |
| DatabricksSparkJar | 1 |

### Jobs Activities

No Databricks Job activities detected.

**Total Databricks Activities**: 215

## 6. Metadata-Driven Pipeline Detection

| Classification | Count |
| --- | --- |
| Yes | 1 |
| Likely | 20 |
| No | 390 |

### Methodology

- **Signal 1**: Lookup -> ForEach -> ExecutePipeline pattern in the pipeline structure
- **Signal 2**: 3+ parameter names matching metadata conventions (tableName, schema, sourceQuery, sinkFolder, loadType, watermarkColumn, sourceTable, sinkTable, sourceConnection, sinkConnection, fileName, filePath, containerPath)
- **Signal 3**: Pipeline uses parameterized datasets (datasets with parameters)

### Classification Rules

- **Yes** = 2 or more signals match
- **Likely** = 1 signal matches
- **No** = 0 signals match

*Note: This is heuristic-based. Metadata-driven pipelines that use unconventional naming or different patterns may be missed.*

## 7. Platform Coverage Notes

- **Synapse**: Pipeline definitions extracted if permissions allow (requires Synapse Artifact User). Synapse supports the same monitoring APIs as ADF (queryPipelineRuns, queryActivityRuns) via the dev.azuresynapse.net endpoint. Currently skipped workspaces are listed with 403 Forbidden.
  - Skipped workspaces: sanketsynapse, aa-pbi-dev, synapse-mohana-lakebridge, lakebridge-profiler-demo, lakehousefeddemo, mglbetest2, synapse-healthcare-advanced-1756964855, andresg-sql-server-dw, sprofiler-test, isaac-synapse, sabademo1, jok-opnt-synapse, nithyatest, demos-mujahid, ct-s01, synapse-migration-demo, atilaksynapsews, demo-ayoub-ws, test-workspace-2, oneenvsql, joksynapsews, karthy-synapse-ws-new, mglbe3, synapse-profiler-workshop-ejp, mms-synapse-ws
- **Fabric**: Pipeline definitions extracted from Fabric workspaces. Fabric has a separate monitoring API (api.fabric.microsoft.com) for pipeline runs but it is not yet integrated. Runtime profiling (runs, costs) is not available for Fabric pipelines.
- **ADF**: Full support -- definitions, runtime profiling, SHIR nodes, cost estimation, and actual cost comparison.
