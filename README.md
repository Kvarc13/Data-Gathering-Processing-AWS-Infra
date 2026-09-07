# BI Data Pipeline — Project Summary

A BI reporting automation platform built entirely on AWS: it extracts data from the data warehouse, production databases, external APIs, and SharePoint files, transforms it using SQL, and delivers ready-to-use reports to Excel spreadsheets on SharePoint and a persistent archive in S3 — entirely automated and on a schedule.

## Problem It Solves
Operational and marketing reports required manual data joining from multiple systems (Redshift, PostgreSQL, Zeropark, external partner APIs, input files from teams) and manual updates of spreadsheets. Every new report meant a new script to write and maintain, and any fix to shared logic required changes in multiple places at once.

## Solution in One Paragraph
A single shared engine (`pipeline-engine`) executes all pipelines; bridges — one per source system — are the only components that contain the credentials and libraries of their respective systems. A single pipeline is not code, but a single YAML configuration file (sources + one joining SQL query + destinations) plus a schedule. Creating a new report = copying a template, filling out the YAML, and running one deploy command. Fixing shared logic = one deploy, and all pipelines benefit immediately.

## Architecture

```text
EventBridge Scheduler (schedule per pipeline)
        │  {"pipeline": "<name>"}
        ▼
pipeline-engine (1 shared Lambda) ── configuration: s3://…/_configs/<name>.yaml
        │
        ├─ redshift-bridge ──── Redshift (data warehouse)               ┐
        ├─ postgres-bridge ──── PostgreSQL (3 prod/replica databases)   │  sources →
        ├─ api-bridge ────────── any HTTP API + secrets                 │  CSV in S3 (raw)
        ├─ sharepoint-reader ── Excel sheets as input                   ┘
        ├─ zeropark-gateway ─── Zeropark reports (asynchronous)
        │
        ├─ duckdb-bridge ────── transformation: ONE SQL query over CSV
        │
        └─ outputs: S3 (processed, files per day) and/or
                    sharepoint-uploader → Excel sheet (replace/append/upsert)
```

### Key Design Decisions:
*   **Configuration as Data.** The pipeline lives in S3, not in code; changing SQL or sources is just a file upload — no build, no CloudFormation required.
*   **Folder Name = Pipeline Identity.** (configuration file, S3 folders, target sheet, schedule) — single source of truth, zero name synchronization needed.
*   **Bridges Isolate Risk.** Secrets are kept exclusively in Secrets Manager, database sessions are enforced as read-only with query timeouts, and network access is handled via dedicated VPCs/Security Groups agreed upon with DevOps.
*   **Waiting Without Paying.** Long-running Zeropark reports do not block the Lambda function: the execution suspends and resumes via a one-time, self-deleting schedule, carrying its state within the event payload.
*   **Fail-Fast.** Configuration is validated before fetching anything, and error messages point to specific fixes; an empty result aborts the run by default, and replace mode refuses to clear a sheet with empty data.
*   **Observability.** Every log line is prefixed with the pipeline name (a shared log group filterable per report), failed runs land in a shared DLQ (after exhaustion of retries) with the pipeline name in the message; sheet writes leave a trace in an "Update Log" tab.

## Technologies

| Layer | Technology |
| :--- | :--- |
| **Compute** | AWS Lambda (Python 3.12) |
| **Scheduling** | Amazon EventBridge Scheduler |
| **Storage** | Amazon S3 (raw with 7-day retention, persistent processed, exports box) |
| **Transformations** | DuckDB (SQL over CSV files in S3, httpfs) |
| **Databases** | Redshift and PostgreSQL via psycopg2, read-only replicas |
| **Secrets** | AWS Secrets Manager (bi/postgres/*, API keys) |
| **SharePoint** | office365 + openpyxl, certificate authentication |
| **Reliability** | SQS DLQ, asynchronous retries, idempotent writes (upsert) |
| **Infrastructure as Code** | CloudFormation / AWS SAM, deploy from VS Code (PowerShell/Bash) |
| **Network** | VPC + dedicated Security Groups for production databases |

## Deployment Status
There are 5 pipelines running in production, covering all source types (Redshift, PostgreSQL, HTTP API, SharePoint Excel, Zeropark reports), feeding team report sheets and a per-day archive in S3 on a daily basis. The old architecture (a copy of the engine per report) has been fully migrated and decommissioned — the shared logic now exists as a single instance.

## Benefits of this Architecture
*   **New report in hours, not days** — without writing or maintaining code; the work boils down to SQL and one YAML file.
*   **Single point of repair** — a fix to the engine or a bridge covers all reports with a single deploy; configuration drift between copies is structurally impossible.
*   **Each new data source is cheaper** — a new PostgreSQL database is just one profile line + a secret; a new API is an entry in the configuration, without any infrastructure changes.
*   **Secure by design** — components with access to production data are read-only, credentials do not appear in code or configuration, and permissions are scoped down to specific paths and functions.
*   **Self-healing reports** — rolling windows + upsert mean that a rerun or backfill overwrites rather than duplicates data; manual interference in a spreadsheet is corrected during the next run.
