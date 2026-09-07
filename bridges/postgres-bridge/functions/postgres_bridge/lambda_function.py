# =============================================================================
# File:     lambda_function.py
# Location: bi-data-pipeline/bridges/postgres-bridge/functions/postgres_bridge/lambda_function.py
# Stack:    postgres-bridge  (deployed ONCE - shared by every pipeline)
# =============================================================================
"""
postgres-bridge - one job: run SQL on a Postgres database and land the
result as a CSV in S3.

Same contract as redshift-bridge, so the engine treats them identically -
a pipeline source just says `type: postgres` and names a connection:

Input event (from pipeline-engine or a manual invoke):
  {
    "sql": "SELECT ... WHERE date BETWEEN '{start}' AND '{end}'",
    "connection": "slave1",                  # profile in CONNECTIONS below
    "window": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"},   # optional if
                                             # the sql has no placeholders
    "bucket": "<raw bucket>",
    "key": "<dataset>/<stamp>/<source>.csv"
  }

Returns: {"key": <key>, "rows": <row count written>}

Placeholders: {start} and {end} render from the window. A literal brace in
SQL (e.g. '{}'::jsonb) must be doubled: '{{}}'. An unknown placeholder or a
placeholder without a window fails fast with a message naming the fix.

CONNECTIONS - one profile per database. Adding a database = one line here
plus its secret; every pipeline can use it immediately. The secret is JSON:
  {"user": "...", "password": "..."}
Never hardcode credentials in this file.

Safety: every session is forced read-only (default_transaction_read_only)
and capped by statement_timeout, so a runaway or accidentally-mutating
query cannot damage the replica. Rows stream server-side in batches to
/tmp then upload - result size is bounded by the 512 MB /tmp, not memory.

Networking: this Lambda lives in the same VPC subnets/security groups as
redshift-bridge - the path to slave1.zeropark.com:5432 proven by the
postgres-connection-test stack on 2026-08-25 (tcp 2 ms).
"""

import csv
import json
import logging
import os

import boto3
import psycopg2

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

s3 = boto3.client('s3', region_name='us-east-1')
secrets_client = boto3.client('secretsmanager', region_name='us-east-1')

# ---------- Connection profiles ----------
# Adding a database = one entry + its secret in Secrets Manager.
CONNECTIONS = {
    'zeropark': {
        'host': 'slave1.zeropark.com',
        'port': 5432,
        'dbname': 'zeropark',   # confirmed in pgAdmin: feed/user/address live here (schema public)
        'secret': 'bi/postgres/zeropark',
    },
    'bingest': {
        'host': 'bingest-cluster.cluster-ro-cvybmwao8fmg.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'postgres',   # >>> confirm - likely a named db on this cluster
        'secret': 'bi/postgres/bingest',
    },
    'accounting': {
        'host': 'infra-accounting-db-replica.ce3lbgyu0hca.us-east-1.rds.amazonaws.com',
        'port': 5432,
        'dbname': 'accounting',   # confirmed in pgAdmin
        'secret': 'bi/postgres/accounting',
    },
    'eventlog': {
        'host': 'zeropark-eventlog-db.zeropark.codewise.com',
        'port': 5432,
        'dbname': 'zeropark_eventlog',
        'secret': 'bi/postgres/eventlog',
    },
}

CONNECT_TIMEOUT = 10          # seconds - fail fast on network/auth problems
STATEMENT_TIMEOUT_MS = 840_000  # 14 min - inside the Lambda's own 15 min cap
BATCH_ROWS = 5_000            # server-side fetch size while streaming to CSV
TMP_CSV = '/tmp/result.csv'


def lambda_handler(event, context):
    job = validate_job(event)
    sql = render_sql(job['sql'], job.get('window'))
    profile = resolve_connection(job['connection'])

    logger.info(f"Connection '{job['connection']}' "
                f"({profile['host']}/{profile['dbname']}): running query")
    connection = connect(profile)
    try:
        with connection:
            # Named cursor = server-side: rows stream in batches instead of
            # materializing the whole result in memory.
            with connection.cursor(name='postgres_bridge') as cursor:
                cursor.itersize = BATCH_ROWS
                cursor.execute(sql)
                rows = write_csv(cursor, TMP_CSV)
    finally:
        connection.close()

    s3.upload_file(TMP_CSV, job['bucket'], job['key'],
                   ExtraArgs={'ContentType': 'text/csv'})
    logger.info(f"{rows} row(s) -> s3://{job['bucket']}/{job['key']}")
    return {'key': job['key'], 'rows': rows}


def validate_job(event: dict) -> dict:
    missing = [field for field in ('sql', 'connection', 'bucket', 'key')
               if not (event or {}).get(field)]
    if missing:
        raise ValueError(f'Job is missing required field(s): '
                         f'{", ".join(missing)} - see the module docstring '
                         f'for the contract')
    return event


def resolve_connection(name: str) -> dict:
    profile = CONNECTIONS.get(name)
    if not profile:
        raise ValueError(f"Unknown connection '{name}'. Known: "
                         f"{', '.join(sorted(CONNECTIONS))} - add a profile "
                         f"to CONNECTIONS in postgres-bridge to extend")
    return profile


def render_sql(sql: str, window: dict | None) -> str:
    """Fill {start}/{end} from the run window; fail fast on anything else."""
    values = dict(window or {})
    try:
        return sql.format_map(_Strict(values, allowed=('start', 'end')))
    except KeyError as error:
        placeholder = error.args[0]
        if placeholder in ('start', 'end'):
            raise ValueError(f'sql uses {{{placeholder}}} but the job has no '
                             f'window - the engine sends one automatically; '
                             f'manual invokes must include it') from None
        raise ValueError(f'Unknown placeholder {{{placeholder}}} in sql - '
                         f'known: {{start}}, {{end}}. A literal brace must '
                         f'be doubled: {{{{...}}}}') from None


class _Strict(dict):
    def __init__(self, values, allowed):
        super().__init__({k: v for k, v in values.items() if k in allowed})

    def __missing__(self, key):
        raise KeyError(key)


def connect(profile: dict):
    credentials = json.loads(secrets_client.get_secret_value(
        SecretId=profile['secret'])['SecretString'])
    return psycopg2.connect(
        host=profile['host'], port=profile['port'], dbname=profile['dbname'],
        user=credentials['user'], password=credentials['password'],
        connect_timeout=CONNECT_TIMEOUT,
        options=(f'-c default_transaction_read_only=on '
                 f'-c statement_timeout={STATEMENT_TIMEOUT_MS}'),
    )


def write_csv(cursor, path: str) -> int:
    """Stream the cursor to a CSV file in batches. Returns the row count.
    Named (server-side) cursors don't contact the server on execute(), so
    cursor.description is None until the first fetch - fetch first."""
    batch = cursor.fetchmany(BATCH_ROWS)
    if cursor.description is None:
        raise ValueError('The query returned no result set - the bridge '
                         'runs SELECTs only')
    with open(path, 'w', newline='') as handle:
        writer = csv.writer(handle)
        writer.writerow([column.name for column in cursor.description])
        rows = 0
        while batch:
            writer.writerows(batch)
            rows += len(batch)
            batch = cursor.fetchmany(BATCH_ROWS)
        return rows
