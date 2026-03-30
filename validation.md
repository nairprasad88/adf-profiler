# ADF Profiler Output - Validation Guide

Every value below comes from the latest output (`2026-03-18_030957`). Use this to spot-check against the Azure Portal and CLI. All factories are in the **field-eng** subscription unless noted otherwise.

> **Lookback window:** The profiler uses a **90-day** window (Dec 18, 2025 – Mar 18, 2026) for both pipeline runs and Cost Management queries. Set portal date filters accordingly — the "Last 30 days" preset will show ~1/3 of these values.

> **Timezone:** ADF Monitor in the Azure Portal displays all timestamps in **UTC**. The Azure CLI and REST API also return timestamps in UTC (ISO 8601 with `Z` suffix). All times referenced in this document are UTC.

> **Pagination:** The `az datafactory pipeline-run query-by-factory` command returns a maximum of 100 runs per call. If the response contains a `continuationToken`, pass it with `--continuation-token` to fetch the next page. The CLI commands below include pagination handling where needed.

> **CSV verification:** Each section includes instructions to independently verify profiler numbers by counting rows in the detailed CSVs under `output/2026-03-18_030957/`. This is the authoritative cross-check — count the rows yourself rather than trusting summary tables alone.

## Output Structure Changes (2026-03-09)

The following changes were made to the output MD files and CSVs:
- **Connector friendly names**: All connector types now use human-readable names (e.g., "Azure Data Lake Storage Gen2" instead of "AzureBlobFS"). File-format connectors like "AzureBlobFS (DelimitedText)" are consolidated into their storage type.
- **Activity categorization in orchestration.md**: Activity types are grouped into Control Flow, Data Movement, Transformation, and External categories.
- **Subscription column**: All factory-level tables now include a Subscription column.
- **Total rows**: All multi-row breakdown tables include a Total row at the bottom.
- **Sort by cost**: Breakdown tables are sorted by cost/activity descending.
- **Empty factory filtering**: overall.md factory-level table only shows factories with at least 1 pipeline, 1 run, or cost > 0.
- **Data volume in factory breakdown**: ingestion.md factory breakdown includes Data Volume (GB) column.
- **Consolidated orchestration sections**: orchestration.md sections 3+6 (subscription) and 3+7 (factory) were merged into single consolidated sections.
- **Actual cost bug fix**: Fixed negative actual cost caused by double-subtracting airflow costs. Actual compute = sum of costs where meterSubcategory is empty.

---

## 1. Pipeline Runs: `adf-sash-demo` / `sash-test-pipe-1`

**Profiler says:** 107 runs - 105 Succeeded, 1 Failed, 1 Cancelled. Avg 3.87 min, max 9.24 min. Total 5,149 activity runs. Estimated cost: **$5.3157**.

**CSV verification:**

```bash
# Count rows in pipeline_runs_detail.csv for this factory (expect: 107)
grep -c 'adf-sash-demo' output/2026-03-18_030957/orchestration/details/pipeline_runs_detail.csv

# Verify status breakdown
grep 'adf-sash-demo' output/2026-03-18_030957/orchestration/details/pipeline_runs_detail.csv | grep -c 'Succeeded'  # expect: 105
grep 'adf-sash-demo' output/2026-03-18_030957/orchestration/details/pipeline_runs_detail.csv | grep -c 'Failed'     # expect: 1
grep 'adf-sash-demo' output/2026-03-18_030957/orchestration/details/pipeline_runs_detail.csv | grep -c 'Cancelled'  # expect: 1
```

Confirmed from `pipeline_runs_by_pipeline.csv` row: `adf-sash-demo,sash-test-pipe-1,107,105,1,1,0,98.1,3.87,9.24,2.74,5149,5.3157`.

**Portal steps:**

1. Go to https://portal.azure.com
2. Search for **adf-sash-demo** in the top search bar, click on the Data Factory
3. Click **Open Azure Data Factory Studio** (blue button)
4. In the left sidebar, click the **Monitor** icon (speedometer)
5. You're on **Pipeline runs**. Set the time filter to **Last 30 days**
6. Count the rows for `sash-test-pipe-1` - should be **107**
7. Check the Status column:
   - **105 green** checkmarks (Succeeded)
   - **1 red X** (Failed) - run from Mar 2 at 06:00 UTC, error: *"ForEach1 failed: inner activity Job1_copy1"*
   - **1 grey** (Cancelled) - run from Mar 2 at 05:30 UTC

**Consumption report for one run:**

8. Hover over any Succeeded run -> click the **consumption icon** (list/meter icon that appears on hover)
9. This opens the consumption report showing **meter quantities consumed** (not prices) — e.g. activity runs count, execution hours by category (Data Movement, Pipeline Activity, External Activity), and the integration runtime used
10. To estimate cost from the consumption report, apply Azure pricing rates to the quantities shown:
    - **Orchestration:** activity runs × $1.00 / 1,000
    - **Data Movement:** DIU-hours × $0.25
    - **Pipeline Activities:** hours × $0.005
    - **External Activities:** hours × $0.00025 (Azure IR) or $0.0001 (Self-hosted IR)
    - All execution charges are prorated by the minute, rounded up
11. For example, if one run shows **48 activity runs** and **2.0333 execution hours** for external activities under Azure IR:
    - Orchestration: 48 × $1.00/1000 = **$0.048**
    - External activity execution: 2.0333 hrs × $0.00025 = **$0.0005**
    - Single-run estimated cost ≈ **$0.049**
    - Extrapolated across 107 runs: ~**$5.24** (close to profiler's $5.32 — difference is due to varying activity counts and trigger runs)

**CLI:**

```bash
# This factory has 107 runs — exceeds 100-run page limit. Must paginate.
RESULT=$(az datafactory pipeline-run query-by-factory \
  --factory-name adf-sash-demo \
  --resource-group pepsi_rg \
  --last-updated-after 2025-12-18T00:00:00Z \
  --last-updated-before 2026-03-18T00:00:00Z \
  -o json)

COUNT=$(echo "$RESULT" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['value']))")
TOKEN=$(echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('continuationToken',''))")

while [ -n "$TOKEN" ]; do
  PAGE=$(az datafactory pipeline-run query-by-factory \
    --factory-name adf-sash-demo \
    --resource-group pepsi_rg \
    --last-updated-after 2025-12-18T00:00:00Z \
    --last-updated-before 2026-03-18T00:00:00Z \
    --continuation-token "$TOKEN" \
    -o json)
  PAGE_COUNT=$(echo "$PAGE" | python3 -c "import sys,json; print(len(json.load(sys.stdin)['value']))")
  COUNT=$((COUNT + PAGE_COUNT))
  TOKEN=$(echo "$PAGE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('continuationToken',''))")
done

echo "Total runs: $COUNT"  # expect: 107
```

> **Note:** ADF only retains pipeline run data for **45 days** — runs older than that won't appear.

---

## 2. Activity Runs inside one `sash-test-pipe-1` run

**Profiler says:** 48.1 activities per run on average (5,149 total / 107 runs). This pipeline has a ForEach that fans out over a variable number of items, so activity counts differ per run.

**Portal steps:**

1. From the Pipeline runs view, click the **pipeline name** link on any Succeeded run
2. This opens the **Activity runs** view
3. Count the rows — the count varies per run (the ForEach iterates over different item counts). The consumption report for one run showed **48 activity runs**, while other runs may show fewer (e.g. ~21)
4. You'll see `ForEach1` and its child activities

**CLI:**

```bash
az datafactory activity-run query-by-pipeline-run \
  --factory-name adf-sash-demo \
  --resource-group pepsi_rg \
  --run-id ad301e9d-fafd-4b5b-ba18-a279269f875d \
  --last-updated-after 2025-12-18T00:00:00Z \
  --last-updated-before 2026-03-18T00:00:00Z \
  --query 'value | length(@)'
```

Returns `21` for this run. Try run `2102c96b-7f4d-4317-9180-c7e12b68d394` (Mar 2 04:30 UTC) for a potentially different count. The profiler's average of **48.1** is computed across all 107 runs.

> **Note:** Activity run queries can also paginate. Check for `continuationToken` in the response if the count seems low.

---

## 3. Failed Pipeline: `bnelsondatafactory` / `pipeline1`

**Profiler says:** 45 runs, 0% success rate (all 45 failed), 1 activity per run. Estimated cost: **$0.0906**. Error: *"Databricks execution failed: Cluster terminated — AZURE_OPERATION_NOT_ALLOWED"*.

**CSV verification:**

```bash
grep -c 'bnelsondatafactory' output/2026-03-18_030957/orchestration/details/pipeline_runs_detail.csv  # expect: 45
grep 'bnelsondatafactory' output/2026-03-18_030957/orchestration/details/pipeline_runs_detail.csv | grep -c 'Failed'  # expect: 45
```

Confirmed from `pipeline_runs_by_pipeline.csv` row: `bnelsondatafactory,pipeline1,45,0,45,0,0,0.0,2.82,4.03,2.12,45,0.0906`.

**Portal steps:**

1. Search for **bnelsondatafactory** in the portal, open the Data Factory (subscription: **field-eng**)
2. Monitor -> Pipeline runs -> Last 30 days
3. You should see **45 runs** of `pipeline1`, all with red X (Failed)
4. Click into any run -> **1 activity run** per execution (`covid append` — a DatabricksNotebook activity)

**CLI:**

```bash
az datafactory pipeline-run query-by-factory \
  --factory-name bnelsondatafactory \
  --resource-group brucenelson \
  --last-updated-after 2025-12-18T00:00:00Z \
  --last-updated-before 2026-03-18T00:00:00Z \
  --query 'value | length(@)'
```

Should return `45`. (Under 100 — no pagination needed.)

---

## 4. Pipeline Runs: `steven-psa-orchestration` / `pipeline_table_ingestion`

**Profiler says:** 1 run, 100% success rate, 2 total activities. Duration 1.09 min. Estimated cost: **$0.0197** (ingestion: $0.0167, orchestration: $0.003).

**CSV verification:**

```bash
grep -c 'steven-psa-orchestration' output/2026-03-18_030957/orchestration/details/pipeline_runs_detail.csv  # expect: 1
grep -c 'steven-psa-orchestration' output/2026-03-18_030957/ingestion/details/copy_activity_details.csv     # expect: 1
```

Confirmed from `pipeline_runs_by_pipeline.csv` row: `steven-psa-orchestration,pipeline_table_ingestion,1,1,0,0,0,100.0,1.09`.
Confirmed from `copy_activity_details.csv`: 1 row — `Copy data1`, source AzureBlobFS, sink AzureBlobFS, DIU 4, cost $0.0167.

**Portal steps:**

1. Search for **steven-psa-orchestration** in the portal, open the Data Factory (subscription: **field-eng**)
2. Monitor -> Pipeline runs -> Last 30 days
3. You should see **1 run** of `pipeline_table_ingestion`, Succeeded
4. Click into the run -> should see **2 activities** including `Copy data1`
5. Click the **Details link (eyeglasses icon)** next to `Copy data1` -> verify source is **AzureBlobFS**, sink is **AzureBlobFS**, DIU used: **4**

**CLI:**

```bash
az datafactory pipeline-run query-by-factory \
  --factory-name steven-psa-orchestration \
  --resource-group field-eng \
  --last-updated-after 2025-12-18T00:00:00Z \
  --last-updated-before 2026-03-18T00:00:00Z \
  --query 'value | length(@)'
```

Should return `1`.

> **Note:** The previous run (2026-03-05 output) showed 2 runs, but 1 run has now fallen outside the 45-day ADF retention window and no longer appears.

---

## 5. SHIR Nodes: `laf-adf`

**Profiler says:** 2 SHIR nodes:
- `integrationRuntime1` -> node `laf-vm-pltest`: Offline, cores Unknown, memory 28.3 GB, CPU 20%
- `integrationRuntime2` -> node `WINDOWS-JDA4569`: Offline, cores Unknown, memory 0.5 GB

**CSV verification:**

```bash
grep 'laf-adf' output/2026-03-18_030957/ingestion/details/shir_nodes.csv
```

Returns 2 rows: `laf-vm-pltest` (28951 MB avail memory) and `WINDOWS-JDA4569` (472 MB avail memory).

**Portal steps:**

1. Search for **laf-adf** in the portal, open the Data Factory
2. Click **Open Azure Data Factory Studio**
3. In the left sidebar, click the **Manage** icon (toolbox/wrench)
4. Under **Connections**, click **Integration runtimes**
5. Find **integrationRuntime1** — type **Self-Hosted**, status **Unavailable** (the profiler reports this as "Offline")
6. Click on it -> **Node Details** section -> confirm node name `laf-vm-pltest`
7. Go back, click **integrationRuntime2** -> confirm `WINDOWS-JDA4569`

**CLI:**

```bash
az datafactory integration-runtime get-status \
  --factory-name laf-adf \
  --resource-group lfurlong3-rg \
  --name integrationRuntime1 \
  --query 'properties.nodes[].{name:nodeName,machine:machineName,maxJobs:maxConcurrentJobs}'
```

---

## 6. SHIR Node: `nshetty-test` / `nshetty-performance-ir`

**Profiler says:** Node `nshetty-perform`, **8 cores** (resolved from VM size `Standard_D8s_v3`), 32 GB memory, memory util 17.8%, CPU 5%.

> **How cores are derived:** The SHIR API does not expose vCPU count directly. The profiler resolves cores in two ways:
> 1. **Preferred (Azure VM match):** The profiler matches the SHIR node's `machineName` to an Azure VM via the Compute API (`virtualMachines`), then looks up the VM size's `numberOfCores` from the [VM Sizes API](https://learn.microsoft.com/en-us/rest/api/compute/virtual-machine-sizes/list) (`locations/{loc}/vmSizes`). This gives the actual vCPU count.
> 2. **Fallback (heuristic):** If the node can't be matched to an Azure VM (e.g., on-premises machines), the profiler reports "Unknown" for cores. The previous `maxConcurrentJobs / 2` heuristic was removed as unreliable.

**CSV verification:**

```bash
grep 'nshetty-test' output/2026-03-18_030957/ingestion/details/shir_nodes.csv
```

Returns: `nshetty-test,nshetty-performance-ir,nshetty-perform,nshetty-perform,Offline,96,8,...,32768.0,Standard_D8s_v3`.

**Portal steps:**

1. Search for **nshetty-test**, open the Data Factory
2. Manage -> Integration runtimes
3. Find **nshetty-performance-ir** — type **Self-Hosted**, status **Unavailable**
4. Click on it -> **Details** tab shows: Running/Registered Node(s) **0/1**, version **5.55.9315.1**
5. Under **Node Details**, confirm node name is **nshetty-perform**, status **Unavailable**, role **Dispatcher/worker**

> **Note:** CPU utilization, available memory, and network are only visible when the node is **Running**. Since this node is Unavailable, the portal shows `---` for those fields.

**CLI — SHIR node info** (works even when node is Unavailable):

```bash
az datafactory integration-runtime get-status \
  --factory-name nshetty-test \
  --resource-group fe-shared-emea-001 \
  --name nshetty-performance-ir \
  --query 'properties.nodes[0].{name:nodeName,machine:machineName,status:status,maxConcurrentJobs:maxConcurrentJobs,version:version}'
```

Should return:
```json
{
  "machine": "nshetty-perform",
  "maxConcurrentJobs": 96,
  "name": "nshetty-perform",
  "status": "Offline",
  "version": "5.55.9315.1"
}
```

**CLI — Verify actual VM cores** (if the VM still exists):

```bash
az vm list-sizes --location eastus \
  --query "[?name=='Standard_D8s_v3'].{name:name, cores:numberOfCores, memoryMB:memoryInMB}" -o json
```

Returns `{"cores": 8, "memoryMB": 32768}` — confirming **8 vCPUs** and **32 GB RAM** for `Standard_D8s_v3`.

> **Note:** The VM `nshetty-perform` has since been deleted, so `az vm list` won't find it. But the VM size spec is static — `Standard_D8s_v3` always has 8 cores and 32 GB regardless of whether the specific VM exists.

---

## 7. Database Instance: `nshetty-test`

**Profiler says:** Linked service `azure_sql_nithya_demo_db`, type AzureSqlDatabase, server `nithya-demo-server.database.windows.net`, database `nithya-demo-db`, using SHIR `nshetty-performance-ir`.

**CSV verification:**

```bash
grep 'nshetty-test' output/2026-03-18_030957/ingestion/details/database_instances.csv
```

Returns: `nshetty-test,azure_sql_nithya_demo_db,AzureSqlDatabase,nithya-demo-server.database.windows.net,nithya-demo-db,nshetty-performance-ir,field-eng,1,0`.

**Portal steps:**

1. Search for **nshetty-test**, open the Data Factory
2. Click **Open Azure Data Factory Studio** -> **Manage** -> **Linked services**
3. Find **azure_sql_nithya_demo_db**
4. Click it - confirm:
   - Type: **Azure SQL Database**
   - Server: **nithya-demo-server.database.windows.net**
   - Database: **nithya-demo-db**
   - Connect via: **nshetty-performance-ir**

**CLI:**

```bash
az datafactory linked-service list \
  --factory-name nshetty-test \
  --resource-group fe-shared-emea-001 \
  --query '[].name' -o tsv
```

Should include `azure_sql_nithya_demo_db`.

---

## 8. Database Instance: `stuart-adf` (SHIR-connected)

**Profiler says:** Linked service `stuartADFSQL`, type SqlServer, server `localhost`, database `adftutorial`, using SHIR `stuartADFSQLlink`. Node `stuartsql`, cores Unknown, memory 4.1 GB, CPU 36%.

**CSV verification:**

```bash
grep 'stuart-adf' output/2026-03-18_030957/ingestion/details/database_instances.csv | grep 'stuartADFSQL'
```

Returns: `stuart-adf,stuartADFSQL,SqlServer,localhost,adftutorial,stuartADFSQLlink,field-eng,2,0`.

**Portal steps:**

1. Search for **stuart-adf**, open the Data Factory
2. Manage -> Linked services -> **stuartADFSQL**
3. Confirm type is **SQL Server**, connect via **stuartADFSQLlink**
4. Go to Integration runtimes -> **stuartADFSQLlink** -> confirm node `stuartsql`, status Offline

---

## 9. Actual Cost Validation: `adf-sash-demo`

Validates that the profiler's actual cost (pulled from the Cost Management API) matches what the Azure portal shows. This is not about estimated vs actual — it's about confirming the profiler correctly queries and sums the Cost Management data.

**Why `adf-sash-demo`:** It's the highest-cost non-Airflow factory in the field-eng subscription, so small rounding errors won't obscure a real discrepancy. It has no Airflow meters to complicate the comparison.

**Profiler says:** Actual cost **$7.7478** (from `cost_comparison.csv`, column `actual_compute`).

**Portal steps:**

1. Go to https://portal.azure.com -> search **Cost Management**
2. Set scope to **field-eng** (subscription `3f2e4d32-8e8d-46d6-82bc-5bb8d962328b`)
3. Click **Cost analysis** in the left sidebar
4. Set date range to **Custom** matching the profiler's 90-day window (check the log for exact start/end timestamps)
5. Click **Add filter** -> Resource type -> **microsoft.datafactory/factories**
6. Click **Add filter** -> Resource -> **adf-sash-demo**
7. Read the **Actual Cost (USD)** total at the top

**Expected result:** The portal total should be within a few cents of the profiler's **$7.75**. Small differences (~$0.05) are normal due to the profiler's time window boundaries (it uses UTC timestamps down to the second, while the portal uses calendar day boundaries).

**If the values don't match:**

- **Off by >$1:** The profiler's Cost Management query may be filtering incorrectly — check that the `ResourceId` matching logic in `_query_subscription_costs` is finding `adf-sash-demo` in the response rows. Add a log line to print matched vs unmatched rows.
- **Profiler shows $0 but portal shows a value:** The profiler likely got a 403 on that subscription's Cost Management API. Check the log for "Cost Management access denied" warnings.
- **Portal shows more meters than expected:** The profiler groups costs by `MeterSubcategory`. Costs where `MeterSubcategory` is empty are counted as compute. Costs with a subcategory containing "airflow" are split into `managed_airflow.csv`. Everything else is excluded from `actual_compute`. Verify this logic matches what you see by grouping by **Meter subcategory** in the portal.

---

## 10. Integration Runtime Counts

**Profiler says:** 37 total IRs - 19 SelfHosted, 16 Managed (Azure), 2 Unknown (Airflow).

**CSV verification:**

```bash
# Count IRs by type from integration_runtimes.csv (exclude header)
tail -n +2 output/2026-03-18_030957/ingestion/details/integration_runtimes.csv | wc -l          # expect: 37 (or count non-AutoResolve + listed ones)
grep -c 'SelfHosted' output/2026-03-18_030957/ingestion/details/integration_runtimes.csv         # expect: 19
grep -c ',Managed,' output/2026-03-18_030957/ingestion/details/integration_runtimes.csv          # expect: 16
grep -c ',Unknown,' output/2026-03-18_030957/ingestion/details/integration_runtimes.csv          # expect: 2
```

**Portal steps:**

1. Open any factory (e.g. `ganeshrj-df-demo`) -> Manage -> Integration runtimes
2. `ganeshrj-df-demo` should show **6 IRs**: `integrationRuntime1` (SelfHosted), `integrationRuntime2` (SelfHosted), `integrationRuntime4` (SelfHosted), `ADBIntRuntime-SH-PL` (Managed), `IR-for-pl` (Managed), `integrationRuntime3` (Managed)
3. The 2 "Unknown" IRs are Airflow runtimes: `profilerTestAirflowRuntime1` (fieldeng-migrations) and `Airflowivylu` (ivylu-demo-airflow)

---

## 11. Factory/Pipeline Counts

**Profiler says:** 203 total Factories/Workspaces, 499 Pipelines, 786 Activities.
- ADF: 377 pipelines, 745 activities (across field-eng and azure-test-pm subscriptions)
- Fabric: 122 pipelines, 41 activities (Fabric workspaces)
- Synapse: 0 pipelines (22 skipped with 403 Forbidden)

**CSV verification:**

```bash
cat output/2026-03-18_030957/overall.csv
# expect: Factory/Workspace Count=203, Pipeline Count=499, Activity Count=786
```

**Portal steps:**

1. Go to Azure Portal -> Data factories -> filter by `field-eng` subscription
2. Count should be close to **157** ADF factories (field-eng sub)
3. For Fabric: go to Fabric portal -> check workspace count matches Fabric workspaces in output

---

## 12. SHIR Node Inventory

**Profiler says:** 7 SHIR nodes across all factories. The following 6 are in the **field-eng** subscription:

| Node Name | Factory | IR Name | Status | Cores | Memory (GB) | Last Connected (UTC) |
|-----------|---------|---------|--------|-------|-------------|----------------------|
| fe-cis-imrans-p | fe-cis-imrans-adf | ire-1 | Offline | Unknown | 4.3 | 2026-03-18 |
| laf-vm-pltest | laf-adf | integrationRuntime1 | Offline | Unknown | 28.3 | 2024-09-24 |
| ganeshrje2vm | ganeshrj-df-demo | integrationRuntime1 | Offline | Unknown | 12.7 | 2022-09-26 |
| WINDOWS-JDA4569 | laf-adf | integrationRuntime2 | Offline | Unknown | 0.5 | 2025-07-24 |
| nshetty-perform | nshetty-test | nshetty-performance-ir | Offline | 8 | 32.0 | 2026-02-02 |
| stuartsql | stuart-adf | stuartADFSQLlink | Offline | Unknown | 4.1 | 2019-11-13 |

> 1 additional node (`EC2AMAZ-D9VIOKG` in `peteradfcuj`) is in the **azure-test-pm** subscription and excluded from portal verification.
>
> Cores show "Unknown" when the node's VM cannot be matched in Azure (deleted, on-premises, or AWS). Only `nshetty-perform` was resolved to `Standard_D8s_v3` (8 cores).

**CSV verification:**

```bash
tail -n +2 output/2026-03-18_030957/ingestion/details/shir_nodes.csv | wc -l  # expect: 7
```

---

## 13. Copy Activity Details: `steven-psa-orchestration`

**Profiler says:** The `pipeline_table_ingestion` run has a copy activity (`Copy data1`), source AzureBlobFS, sink AzureBlobFS, DIU used: 4, IR: `AutoResolveIntegrationRuntime (East US)`. Data read: ~24 KB, rows: 8. Ingestion cost: **$0.0167**.

**CSV verification:**

```bash
grep 'steven-psa-orchestration' output/2026-03-18_030957/ingestion/details/copy_activity_details.csv
```

Returns 1 row: `Copy data1`, source `AzureBlobFS`, sink `AzureBlobFS`, 24616 bytes read, 1588 bytes written, 8 rows, DIU 4, cost formula `4 DIU x 0.0167h x $0.25/DIU-h = $0.0167`.

**Portal steps:**

1. Open **steven-psa-orchestration** -> Monitor -> Pipeline runs
2. Click into the succeeded run (Feb 24)
3. Click the **Details link (eyeglasses icon)** next to `Copy data1`
4. Verify: Source type **AzureBlobFS**, Sink type **AzureBlobFS**, DIU: **4**
5. Check data read/written matches profiler values (~24 KB read, ~1.5 KB written, 8 rows)

---

## Quick CLI Commands

All commands below target the **field-eng** subscription. Single quotes around JMESPath queries avoid zsh escaping issues. ADF retains run data for **45 days only** — use Azure Monitor diagnostic logs for longer retention. All timestamps are in **UTC**.

> **Pagination:** `query-by-factory` returns max 100 runs per call. For factories with >100 runs (`adf-sash-demo`), use the pagination script from Section 1. For factories with ≤100 runs, the simple `--query 'value | length(@)'` is sufficient.

```bash
# Pipeline run count for adf-sash-demo (expect: 107 — PAGINATE)
# See Section 1 for full pagination script

# Pipeline run count for bnelsondatafactory (expect: 45)
az datafactory pipeline-run query-by-factory \
  --factory-name bnelsondatafactory \
  --resource-group brucenelson \
  --last-updated-after 2025-12-18T00:00:00Z \
  --last-updated-before 2026-03-18T00:00:00Z \
  --query 'value | length(@)'

# Pipeline run count for steven-psa-orchestration (expect: 1)
az datafactory pipeline-run query-by-factory \
  --factory-name steven-psa-orchestration \
  --resource-group field-eng \
  --last-updated-after 2025-12-18T00:00:00Z \
  --last-updated-before 2026-03-18T00:00:00Z \
  --query 'value | length(@)'

# SHIR status for laf-adf
az datafactory integration-runtime get-status \
  --factory-name laf-adf \
  --resource-group lfurlong3-rg \
  --name integrationRuntime1 \
  --query 'properties.nodes[].{name:nodeName,maxJobs:maxConcurrentJobs}'

# Linked services for nshetty-test (expect: azure_sql_nithya_demo_db)
az datafactory linked-service list \
  --factory-name nshetty-test \
  --resource-group fe-shared-emea-001 \
  --query '[].name' -o tsv
```

---

## Math Spot-Check

All numbers below are derived by counting rows in the detail CSVs under `output/2026-03-18_030957/` and cross-checked against the profiler summary tables.

| What | Calculation | Expected | Report Shows |
|------|------------|----------|-------------|
| sash orchestration | 5,256 events x $1.00/1000 | $5.2560 | $5.2560 |
| sash pipeline activities | ~5149 acts x tiny hourly rate | $0.0596 | $0.0596 |
| sash total | $5.256 + $0.0596 | $5.3156 | $5.3157 |
| bnelson orchestration | 90 events x $1.00/1000 | $0.0900 | $0.09 |
| bnelson total | $0.09 + $0.0006 | $0.0906 | $0.0906 |
| steven-psa ingestion | 1 run x 1 copy x 4 DIU | $0.0167 | $0.0167 |
| steven-psa total | $0.003 + $0.0167 | $0.0197 | $0.0197 |
| Airflow cost (fieldeng-migrations) | Azure managed airflow | $646.0549 | $646.0549 |
| Airflow cost (ivylu-demo-airflow) | Azure managed airflow | $635.9220 | $635.9220 |
| Total Airflow | Sum | $1,281.98 | $1,281.98 |
| Total actual billed (excl Airflow) | Sum of compute costs | $65.08 | $65.08 |
| Total estimated | Sum of all pipelines | $9.58 | $9.58 |
| SHIR cores (nshetty-perform) | VM size Standard_D8s_v3 | 8 | 8 |
| SHIR cores (all others) | VMs not found / deleted | Unknown | Unknown |
