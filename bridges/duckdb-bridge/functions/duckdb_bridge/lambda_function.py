# =============================================================================
# File:     lambda_function.py
# Location: bi-automation/lambdas/zeropark/Automations_AWS/bridges/duckdb-bridge/functions/duckdb_bridge/lambda_function.py
# =============================================================================
"""
duckdb-bridge - the ONE Lambda that runs SQL over S3 CSVs.

Deployed once, shared by every data-fetching engine: it carries the
duckdb-layer (pinned HERE, in one place, instead of in every pipeline) and
the httpfs setup. A caller sends SQL with {placeholders}, a map of
placeholder -> s3:// CSV, and an S3 destination; the result lands as a CSV.

Event contract:
  {
    "sql":    "SELECT ... FROM read_csv_auto('{zeropark}') ...
               WHERE d BETWEEN '{start}' AND '{end}'",
    "files":  {"zeropark": "s3://raw-.../zp.csv",
               "intango":  "s3://raw-.../intango.csv"},
    "window": {"start": "2026-08-04", "end": "2026-08-10"},   # optional
    "bucket": "raw-data-imports-<account>",
    "key":    "MyDataset/2026-08-10/_combined.csv",
    "split":  {"bucket": "processed-reports-<account>",       # optional:
               "prefix": "MyDataset/"}    # ALSO write one file per value of
                                          # the FIRST result column (per-day
                                          # files: <prefix><value>.csv)
  }
Returns:
  {"key": "...", "rows": N, "split_keys": [...]}   # split_keys only if split

Placeholders are substituted by plain text replacement ({name} -> URI,
{start}/{end} -> window dates); an unknown {placeholder} in the SQL fails
loudly with the list of known ones. Other braces in the SQL are tolerated.

ALL data processing happens in SQL: the combine IS the caller's query and
the optional split runs as per-value `COPY ... WHERE` statements over a
temp table - DuckDB streams both, so quoting and memory are its problem,
not this code's. Results stage in /tmp (spill directory for big sets).
"""

import logging
import os
import re
import shutil

import boto3
import duckdb

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

s3 = boto3.client('s3', region_name='us-east-1')

TMP_CSV = '/tmp/result.csv'
TMP_SPLIT = '/tmp/split.csv'
PLACEHOLDER = re.compile(r'\{([A-Za-z_][A-Za-z0-9_]*)\}')


# ============================================================================
# Lambda entry point  ──  describes the whole job in one screen
# ============================================================================

def lambda_handler(event, context):
    job = parse_job(event)
    logger.info(f"SQL over {sorted(job['files'])} -> s3://{job['bucket']}/{job['key']}")

    connection = init_duckdb()
    connection.execute(f"CREATE TEMP TABLE result AS ({job['sql']})")
    row_count = connection.execute(
        f"COPY (SELECT * FROM result) TO '{TMP_CSV}' (HEADER, DELIMITER ',')").fetchone()[0]
    upload_file_to_s3(TMP_CSV, job['bucket'], job['key'])
    split_keys = split_via_sql(connection, job['split']) if job.get('split') else None
    connection.close()

    logger.info(f"{row_count} row(s) -> s3://{job['bucket']}/{job['key']}"
                + (f' + {len(split_keys)} split file(s)' if split_keys else ''))
    result = {'key': job['key'], 'rows': row_count}
    return {**result, 'split_keys': split_keys} if split_keys is not None else result


# ============================================================================
# Step 0: The job  ──  validate the event, render the SQL
# ============================================================================

def parse_job(event: dict) -> dict:
    missing = [field for field in ('sql', 'files', 'bucket', 'key')
               if not event.get(field)]
    if missing:
        raise ValueError(f'Missing event field(s): {", ".join(missing)}')
    if event.get('split') and not (event['split'].get('bucket')
                                   and event['split'].get('prefix')):
        raise ValueError("'split' needs 'bucket' and 'prefix'")
    return {**event, 'sql': render_sql(event['sql'], event['files'],
                                       event.get('window'))}


def render_sql(sql: str, files: dict, window: dict | None) -> str:
    """{name} -> s3:// URI, {start}/{end} -> window dates, by plain text
    replacement. Unknown placeholders fail loudly BEFORE DuckDB runs."""
    known = dict(files)
    if window:
        known |= {'start': window['start'], 'end': window['end']}

    unknown = set(PLACEHOLDER.findall(sql)) - set(known)
    if unknown:
        raise ValueError(f'SQL references unknown placeholders '
                         f'{sorted(unknown)} - known: {sorted(known)}')

    for name, value in known.items():
        sql = sql.replace('{' + name + '}', str(value))
    return sql


# ============================================================================
# Step 1: Run the SQL  ──  DuckDB (httpfs) reads S3, writes /tmp
# ============================================================================

def init_duckdb():
    """httpfs comes pre-installed in the duckdb-layer (/opt/.duckdb) - copy
    to /tmp on cold start and LOAD, same as zeropark-query-bridge. S3 auth uses
    this Lambda's own credentials from the runtime env."""
    if os.path.exists('/opt/.duckdb') and not os.path.exists('/tmp/.duckdb'):
        shutil.copytree('/opt/.duckdb', '/tmp/.duckdb')
    con = duckdb.connect()
    con.execute("SET home_directory = '/tmp'")
    con.execute("SET temp_directory = '/tmp'")   # spill for big result sets
    try:
        con.execute("SET autoinstall_known_extensions = false; LOAD httpfs;")
    except duckdb.Error:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region = 'us-east-1'; SET s3_url_style = 'path';")
    con.execute(f"""
        SET s3_access_key_id     = '{os.environ.get('AWS_ACCESS_KEY_ID', '')}';
        SET s3_secret_access_key = '{os.environ.get('AWS_SECRET_ACCESS_KEY', '')}';
        SET s3_session_token     = '{os.environ.get('AWS_SESSION_TOKEN', '')}';
    """)
    return con


# ============================================================================
# Step 2: Upload  ──  full result + optional per-first-column files
# ============================================================================

def upload_file_to_s3(path: str, bucket: str, key: str) -> None:
    with open(path, 'rb') as csv_file:
        s3.upload_fileobj(csv_file, bucket, key,
                          ExtraArgs={'ContentType': 'text/csv'})


def split_via_sql(connection, split: dict) -> list[str]:
    """One extra file per distinct value of the FIRST result column
    (<prefix><value>.csv). Meant for per-day processed files: overlapping
    rolling windows then overwrite, never duplicate. Done IN SQL
    (per-value COPY ... WHERE over the temp table), so quoted values,
    commas inside fields and result size are DuckDB's problem, not ours."""
    first_column = connection.execute('SELECT * FROM result LIMIT 0').description[0][0]
    values = [row[0] for row in connection.execute(
        f'SELECT DISTINCT "{first_column}" FROM result ORDER BY 1').fetchall()]

    keys = []
    for value in values:
        literal = 'NULL' if value is None else "'" + str(value).replace("'", "''") + "'"
        connection.execute(
            f'COPY (SELECT * FROM result WHERE "{first_column}" '
            f"IS NOT DISTINCT FROM {literal}) "
            f"TO '{TMP_SPLIT}' (HEADER, DELIMITER ',')")
        key = f"{split['prefix']}{value}.csv"
        upload_file_to_s3(TMP_SPLIT, split['bucket'], key)
        keys.append(key)
    return keys
