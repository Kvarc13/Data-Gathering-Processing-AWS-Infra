# =============================================================================
# File:     lambda_function.py
# Location: bi-automation/lambdas/zeropark/Automations_AWS/bridges/redshift-bridge/functions/redshift_bridge/lambda_function.py
# =============================================================================
"""
redshift-bridge - the ONE Lambda that talks to Redshift.

Deployed once, shared by every data-fetching engine: it carries psycopg2 and
sits in the VPC with warehouse access, so the engines stay light and
VPC-free. An engine invokes it with SQL, a connection profile name and an S3
destination; the result lands as a CSV.

Event contract:
  {
    "sql":        "SELECT ... WHERE d BETWEEN '{start}' AND '{end}'",
    "connection": "warehouse",          # profile NAME from CONNECTIONS below
    "window":     {"start": "2026-08-04", "end": "2026-08-10"},  # optional -
                  # substituted into {start}/{end} in the SQL (plain text
                  # replacement, tolerates other braces in the SQL)
    "bucket":     "raw-data-imports-<account>",
    "key":        "MyDataset/2026-08-10/feeds.csv"
  }
Returns:
  {"key": "...", "rows": N}

Connection profiles (host, database, port, credentials secret) are defined
ONCE here in CONNECTIONS - callers only name one. Adding a database or
rotating a secret = edit this file; every pipeline gets it. Nothing
sensitive transits a payload, not even a secret name.

Cross-account secrets: the warehouse credentials are the Redshift-MANAGED
secret (redshift!...) in the PROD account, encrypted with the AWS-managed
key - it CANNOT be read cross-account directly. Profiles with an
'assume_role' therefore read the secret with THAT role's temporary
credentials (sts:AssumeRole first), the exact pattern
RedshiftUserExportLambda already uses. Profiles without 'assume_role' read
with this Lambda's own credentials (same-account secrets).

Memory: the cursor streams to /tmp in batches, then uploads to S3 - constant
Python memory; result size is bounded by ephemeral storage (2 GB configured
in template.yaml, raise it there for bigger pulls).
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
sts_client = boto3.client('sts', region_name='us-east-1')

FETCH_BATCH_SIZE = 5000
TMP_CSV = '/tmp/result.csv'

# ---------- Connection profiles (edit here, in ONE place) ----------
# secret_arn:   full ARN (cross-account reads require it)
# assume_role:  prod role read the secret WITH (see the docstring); omit
#               for same-account secrets

CONNECTIONS = {
    'warehouse': {
        'host': 'bi-warehouse.978438254669.us-east-1.redshift-serverless.amazonaws.com',
        'dbname': 'warehouse',
        'port': 5439,
        'secret_arn': 'arn:aws:secretsmanager:us-east-1:978438254669:secret:redshift!bi-warehouse-bi-admin-CsFFZ9',
        'assume_role': 'arn:aws:iam::978438254669:role/automations-zeropark-prod-secrets-reader',
    },
    'accounting': {
        'host': 'bi-warehouse.978438254669.us-east-1.redshift-serverless.amazonaws.com',
        'dbname': 'accounting',
        'port': 5439,
        'secret_arn': 'arn:aws:secretsmanager:us-east-1:978438254669:secret:redshift!bi-warehouse-bi-admin-CsFFZ9',
        'assume_role': 'arn:aws:iam::978438254669:role/automations-zeropark-prod-secrets-reader',
    },
}


# ============================================================================
# Lambda entry point  ──  describes the whole job in one screen
# ============================================================================

def lambda_handler(event, context):
    job = parse_job(event)
    logger.info(f"{job['connection']['host']}/{job['connection']['dbname']} "
                f"-> s3://{job['bucket']}/{job['key']}")

    credentials = load_credentials(job['connection'])
    with redshift_connection(job['connection'], credentials) as connection:
        row_count = query_to_csv_file(connection, job['sql'], TMP_CSV)
    upload_csv_to_s3(TMP_CSV, job['bucket'], job['key'])

    logger.info(f"{row_count} row(s) -> s3://{job['bucket']}/{job['key']}")
    return {'key': job['key'], 'rows': row_count}


# ============================================================================
# Step 0: The job  ──  validate the event, resolve the profile, render dates
# ============================================================================

def parse_job(event: dict) -> dict:
    missing = [field for field in ('sql', 'connection', 'bucket', 'key')
               if not event.get(field)]
    if missing:
        raise ValueError(f'Missing event field(s): {", ".join(missing)}')

    profile = event['connection']
    if profile not in CONNECTIONS:
        raise ValueError(f"Unknown connection profile '{profile}'. "
                         f"Known: {', '.join(sorted(CONNECTIONS))}")

    return {**event,
            'connection': CONNECTIONS[profile],
            'sql': render_sql(event['sql'], event.get('window'))}


def render_sql(sql: str, window: dict | None) -> str:
    """Substitute {start}/{end} by plain text replacement - unlike
    str.format(), this tolerates any other braces the SQL may contain."""
    if not window:
        return sql
    return sql.replace('{start}', window['start']).replace('{end}', window['end'])


# ============================================================================
# Step 1: Query Redshift  ──  stream the cursor into a CSV file
# ============================================================================

def redshift_connection(connection_info: dict, credentials: dict):
    """Context manager so we always close the connection, even on errors."""
    return _RedshiftConnection(connection_info, credentials)


class _RedshiftConnection:
    def __init__(self, connection_info: dict, credentials: dict):
        self._connection_info = connection_info
        self._credentials = credentials
        self._connection = None

    def __enter__(self):
        try:
            self._connection = psycopg2.connect(
                host=self._connection_info['host'],
                dbname=self._connection_info['dbname'],
                port=self._connection_info.get('port', 5439),
                user=self._credentials['username'],
                password=self._credentials['password'],
            )
            return self._connection
        except psycopg2.Error as error:
            logger.error(f'Cannot connect to Redshift: {error}')
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        if self._connection is not None:
            self._connection.close()


def query_to_csv_file(connection, sql: str, path: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        return stream_cursor_to_csv(cursor, path)


def stream_cursor_to_csv(cursor, path: str) -> int:
    """Header from the cursor description, then rows in batches - no pandas,
    constant memory. Returns the number of data rows written."""
    with open(path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow([column.name for column in cursor.description])
        row_count = 0
        while batch := cursor.fetchmany(FETCH_BATCH_SIZE):
            writer.writerows(batch)
            row_count += len(batch)
    return row_count


# ============================================================================
# Step 2: Upload to S3
# ============================================================================

def upload_csv_to_s3(path: str, bucket: str, key: str) -> None:
    with open(path, 'rb') as csv_file:
        s3.upload_fileobj(csv_file, bucket, key,
                          ExtraArgs={'ContentType': 'text/csv'})


# ============================================================================
# AWS Secrets Manager  (shared utility)
# ============================================================================

def load_credentials(connection: dict) -> dict:
    """The warehouse secret: {"username": ..., "password": ...}. With
    'assume_role' set, the secret is read with THAT role's temporary
    credentials (cross-account redshift!-managed secrets); otherwise with
    this Lambda's own."""
    client = secrets_client_for(connection.get('assume_role'))
    response = client.get_secret_value(SecretId=connection['secret_arn'])
    return json.loads(response['SecretString'], strict=False)


def secrets_client_for(role_arn: str | None):
    if not role_arn:
        return secrets_client
    credentials = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName='redshift-bridge')['Credentials']
    return boto3.client('secretsmanager', region_name='us-east-1',
                        aws_access_key_id=credentials['AccessKeyId'],
                        aws_secret_access_key=credentials['SecretAccessKey'],
                        aws_session_token=credentials['SessionToken'])