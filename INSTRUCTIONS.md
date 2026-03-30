# ADF Profiler

Scans your Azure Data Factory environment and generates a full report: pipelines, databases, tables, SHIR infrastructure, pipeline run history, activity costs, and actual vs estimated billing.

---

## Quick Start

### Step 1: Install Azure CLI

- **Mac:** `brew install azure-cli`
- **Windows:** `winget install Microsoft.AzureCLI`
- **Linux:** `curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash`

### Step 2: Log in to Azure

```bash
az login
```

This opens your browser. Sign in with your Azure account.

### Step 3: Install Python dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Run it

```bash
python extract_pipelines.py
```

That's it. Output goes to the `./output/` folder.

---

## Options

| Flag | What it does | Example |
|------|-------------|---------|
| `--subscription-id` | Only scan one subscription | `--subscription-id abc-123` |
| `--resource-group` | Only scan one resource group | `--resource-group my-rg` |
| `--factory-name` | Only scan one factory | `--factory-name my-factory` |
| `--days` | How far back to look at pipeline runs (default: 7) | `--days 30` |
| `--output-dir` | Where to save results (default: `./output`) | `--output-dir ./results` |
| `--no-synapse` | Skip Synapse workspaces | |
| `--no-fabric` | Skip Fabric workspaces | |

### Examples

```bash
# Scan everything with 30-day lookback
python extract_pipelines.py --days 30

# Scan one specific factory
python extract_pipelines.py --factory-name my-factory --resource-group my-rg

# ADF only, skip Synapse and Fabric
python extract_pipelines.py --no-synapse --no-fabric
```

---

## What You Need (Permissions)

| What | Role needed | Why |
|------|------------|-----|
| **Read factories & pipelines** | Reader on subscription | Lists factories, pipelines, datasets, etc. |
| **Run profiling** | Data Factory Contributor on factories | Queries pipeline runs, activity runs, SHIR status |
| **See actual costs** | Billing Reader on subscription | Pulls real billed costs from Azure Cost Management |
| **Synapse workspaces** | Synapse Artifact User or Synapse Artifact Publisher on each workspace | Required to read pipelines, datasets, linked services, etc. via the Synapse dev endpoint. Synapse Administrator or Synapse Contributor also work but grant broader access than needed. Without this, Synapse workspaces are skipped with 403 Forbidden. Use `--no-synapse` to skip Synapse entirely. |

If you're missing a permission, the tool will tell you in the output — it won't crash.

---

## What You Get

```
output/YYYY-MM-DD_HHMMSS/
├── overall.csv + .md             # Top-level rollup of all metrics
├── cost_comparison.csv + .md     # Estimated vs actual costs
├── ingestion/
│   ├── ingestion.csv + .md       # Ingestion track rollup
│   └── details/                  # Ingestion detail files
├── orchestration/
│   ├── orchestration.csv + .md   # Orchestration track rollup
│   └── details/                  # Orchestration detail files
└── summary_report.csv            # Everything in one place
```

---

## About Cost Estimates

The tool estimates costs using **Azure list rates** (retail prices before any discounts). It auto-detects your factory's region and tries to pull live rates from the Azure Retail Prices API. If that's not available, it uses standard rates.

**Why the estimate might differ from your actual bill:**
- Your organization may have **enterprise agreements**, **reserved capacity**, or **volume discounts** that lower the actual cost
- The tool can't read costs for factories where you don't have **Billing Reader** access — those show up as "Missing Permissions" in the cost breakdown

**Note on infrastructure costs:** Actual cost figures in the profiler output **exclude infrastructure costs** (Managed Airflow, Private Link, and other infra line items). Only compute and orchestration actuals are shown, so that estimated vs actual is an apples-to-apples comparison. Infrastructure costs still appear in your Azure bill but are not part of the profiler's cost comparison.

---

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `az login` says expired or not found | Run `az login` again |
| `403 Forbidden` errors | You need more permissions — ask your Azure admin for the roles listed above |
| `SSLCertVerificationError` | Run `pip install --upgrade certifi` |
| Profiling takes too long | Use `--days 3` to scan fewer days |
| No pipeline runs found | Increase `--days` or check if the factory has recent activity |
| Cost Management access denied | You need Billing Reader on the subscription (optional — everything else still works) |

---

## Service Principal Auth (for automation)

If you're running this from a CI/CD pipeline or scheduled job:

```bash
python extract_pipelines.py \
  --tenant-id YOUR_TENANT_ID \
  --client-id YOUR_CLIENT_ID \
  --client-secret YOUR_CLIENT_SECRET
```
