# =============================================================================
# File:     lambda_function.py
# Location: bi-data-pipeline/pipeline-engine/functions/engine/lambda_function.py
# Stack:    pipeline-engine  (deployed ONCE - shared by every pipeline)
# =============================================================================
"""
pipeline-engine - the ONE Lambda that runs report pipelines.

Bridges do I/O, this engine orchestrates (see the project README). Deployed
once, like the bridges. A pipeline is NOT code - it is a YAML config in S3:

    s3://<processed bucket>/_configs/<pipeline name>.yaml

The pipeline's EventBridge schedule (its whole stack - see
pipelines/_template/) invokes this function with {"pipeline": "<name>"}.
The name is the single source of pipeline identity: it is the config file
name, the folder in the raw/processed buckets and the SharePoint bridge
inbox folder. Nothing to keep in sync - there is exactly one copy of it.

Config file keys (pipelines/_template/pipeline.yaml is a commented skeleton):
  days_back            window: last N days ending yesterday (default 1)
  fail_on_empty        abort when combine_sql returns 0 rows (default true)
  sources              {name: source spec} - each lands ONE CSV in raw;
                       the name is that CSV's {placeholder} in combine_sql
  combine_sql          ONE DuckDB query over the raw CSVs
  outputs              processed_s3 and/or sharepoint_bridge, each optional

Event contracts:
  scheduled/manual: {"pipeline": "my-report"}
  backfill:         {"pipeline": "my-report",
                     "start_date": "YYYY-MM-DD", "end_date": "YYYY-MM-DD"}
  resume:           {"pipeline": "my-report", "resume": {...run state...}}

Waiting without paying: Zeropark reports are scheduled first, other sources
download while they build, then ONE status check each. Still pending -> this
invocation ENDS and a one-time self-deleting EventBridge schedule re-invokes
the engine with the run state - no DynamoDB, no sleeping. Manual backfills
may therefore return {'status': 'suspended'} and finish in a later
invocation (follow the run in CloudWatch).

Failures: a run that fails all of Lambda's async retries lands in this
stack's shared DLQ; the dead message's payload names the pipeline.

ADDING A SOURCE TYPE = one job-builder function + one line in BRIDGES
(plus, for a NEW bridge, its ARN env var + invoke permission in
template.yaml). A new Postgres DATABASE needs no engine change at all:
add a connection profile to postgres-bridge's CONNECTIONS + its secret.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
import yaml
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

s3 = boto3.client('s3', region_name='us-east-1')
# Bridges legally run up to 900 s; botocore's defaults (read_timeout=60,
# automatic retries) would abort a long sync invoke AND re-run it - i.e.
# execute the same warehouse query twice. One run = one query:
lambda_client = boto3.client('lambda', region_name='us-east-1',
                             config=Config(read_timeout=910,
                                           connect_timeout=10,
                                           retries={'max_attempts': 0}))
scheduler_client = boto3.client('scheduler', region_name='us-east-1')

# ---------- Wiring from template.yaml (one engine = one set, always present) --

RAW_BUCKET = os.environ['RAW_BUCKET']              # 7-day retention, staging
PROCESSED_BUCKET = os.environ['PROCESSED_BUCKET']  # permanent record + configs
EXPORT_BUCKET = os.environ['EXPORT_BUCKET']        # SharePoint bridge inbox
GATEWAY_ARN = os.environ['GATEWAY_FUNCTION_ARN']
DUCKDB_BRIDGE_ARN = os.environ['DUCKDB_BRIDGE_ARN']
API_BRIDGE_ARN = os.environ['API_BRIDGE_ARN']
REDSHIFT_BRIDGE_ARN = os.environ['REDSHIFT_BRIDGE_ARN']
POSTGRES_BRIDGE_ARN = os.environ['POSTGRES_BRIDGE_ARN']
SHAREPOINT_READER_ARN = os.environ['SHAREPOINT_READER_ARN']
SCHEDULER_ROLE_ARN = os.environ['SCHEDULER_ROLE_ARN']

CONFIG_PREFIX = '_configs/'

DEFAULTS = {
    'days_back': 1,
    'fail_on_empty': True,
    'resume_delay_minutes': 3,
    'max_resume_attempts': 10,
    'outputs': {},
}


# =============================================================================
# Lambda entry point  ──  describes the whole job in one screen
# =============================================================================
# A run either finishes in one invocation, or - when a Zeropark report is
# still generating - suspends and continues in a scheduler-fired resume
# invocation carrying the run state. Both paths meet in continue_or_suspend.

def lambda_handler(event, context):
    if not (event or {}).get('pipeline'):
        raise ValueError('Event needs {"pipeline": "<name>"} - the schedule '
                         'and deploy.sh set it from the pipeline folder name')
    bind_log_prefix(event['pipeline'])
    cfg = load_config(event['pipeline'])
    if 'resume' in event:
        return resume_run(cfg, event['resume'], context)
    return start_run(cfg, event, context)


def bind_log_prefix(pipeline: str) -> None:
    """Prefix every log line of this invocation with [<pipeline>], so the
    shared log group filters cleanly per pipeline:
      aws logs tail /aws/lambda/pipeline-engine --filter-pattern '"[<name>]"'
    Safe as a global swap: a Lambda container runs ONE invocation at a time."""
    global logger

    class _Prefixed(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            return f'[{self.extra["pipeline"]}] {msg}', kwargs

    logger = _Prefixed(logging.getLogger(), {'pipeline': pipeline})


def start_run(cfg: dict, event: dict, context) -> dict:
    window = resolve_window(cfg, event)
    sources = enabled_sources(cfg)
    logger.info(f'{cfg["name"]}: {window["start"]} -> {window["end"]} '
                f'(inclusive, UTC); sources: {", ".join(sources)}')

    state = {
        'window': window,
        'pending': schedule_zeropark_reports(cfg, sources, window),
        'raw_files': fetch_all_sources(cfg, sources, window),        # Step 1
        'attempts': 0,
    }
    return continue_or_suspend(cfg, state, context)


def resume_run(cfg: dict, state: dict, context) -> dict:
    logger.info(f'{cfg["name"]}: resumed '
                f'(attempt {state["attempts"]}/{cfg["max_resume_attempts"]}); '
                f'pending: {", ".join(state["pending"])}')
    return continue_or_suspend(cfg, state, context)


def continue_or_suspend(cfg: dict, state: dict, context) -> dict:
    collect_ready_reports(cfg, state)         # one status check per report
    if state['pending']:
        return suspend_run(cfg, state, context)   # re-invoked by the scheduler
    return finish_run(cfg, state)


def finish_run(cfg: dict, state: dict) -> dict:
    combined = combine_raw_files(cfg, state['raw_files'], state['window'])  # Step 2
    ensure_not_empty(cfg, combined['rows'])
    shipped = ship_outputs(cfg, combined, state['window'])                  # Step 3
    logger.info(f'{combined["rows"]} row(s) combined -> {shipped}')
    return {'rows': combined['rows'],
            'range': [state['window']['start'], state['window']['end']],
            'raw_files': state['raw_files'], **shipped}


# =============================================================================
# Step 0: The pipeline  ──  load its config from S3, fail fast if broken
# =============================================================================

def load_config(name: str) -> dict:
    key = f'{CONFIG_PREFIX}{name}.yaml'
    try:
        body = s3.get_object(Bucket=PROCESSED_BUCKET, Key=key)['Body'].read()
    except s3.exceptions.NoSuchKey:
        raise RuntimeError(f'No pipeline config at s3://{PROCESSED_BUCKET}/'
                           f'{key} - the pipeline folder\'s deploy.sh '
                           f'uploads it') from None
    cfg = {**DEFAULTS, **yaml.safe_load(body), 'name': name}
    validate_config(cfg)
    return cfg


def validate_config(cfg: dict) -> None:
    sources = enabled_sources(cfg)
    if not sources:
        raise RuntimeError('No sources enabled - nothing to fetch')
    if not cfg.get('combine_sql'):
        raise RuntimeError("'combine_sql' is missing - every pipeline "
                           "combines, even one source: "
                           "SELECT * FROM read_csv_auto('{that_source}')")
    known = sorted(BRIDGES) + ['zeropark']
    for name, source in sources.items():
        if source.get('type') not in known:
            raise RuntimeError(f"Source '{name}': unknown type "
                               f"'{source.get('type')}'. Known: {known}")
    sharepoint = cfg['outputs'].get('sharepoint_bridge', {})
    if sharepoint.get('enabled'):
        jobs = sharepoint.get('jobs') or [sharepoint.get('job') or {}]
        for job in jobs:
            if not job.get('target-file'):
                raise RuntimeError("sharepoint_bridge output is enabled but "
                                   "a job has no 'target-file' - fill it in "
                                   "pipeline.yaml")


def resolve_window(cfg: dict, event: dict) -> dict:
    """Explicit backfill dates win; otherwise the last days_back days ending
    yesterday (UTC). The stamp names this run's folder in the raw bucket."""
    if event.get('start_date'):
        start, end = event['start_date'], event['end_date']
    else:
        today = datetime.now(timezone.utc).date()
        start = (today - timedelta(days=cfg['days_back'])).isoformat()
        end = (today - timedelta(days=1)).isoformat()
    return {'start': start, 'end': end,
            'stamp': end if start == end else f'{start}_{end}'}


def enabled_sources(cfg: dict) -> dict:
    return {name: source for name, source in cfg['sources'].items()
            if source.get('enabled', True)}


def ensure_not_empty(cfg: dict, row_count: int) -> None:
    if not row_count and cfg['fail_on_empty']:
        raise RuntimeError('combine_sql returned 0 rows - aborting '
                           '(set fail_on_empty: false to allow)')


# =============================================================================
# Step 1: Fetch  ──  every source = a small job for the bridge that owns it
# =============================================================================
# A source type is (bridge ARN, job builder). The builder returns the
# bridge-specific payload fields; bucket/key are added uniformly below.

def api_job(source: dict, window: dict) -> dict:
    return {'request': source['request'], 'window': window}


def sql_job(source: dict, window: dict) -> dict:
    """redshift and postgres share one contract: sql + connection profile."""
    return {'sql': source['sql'], 'connection': source['connection'],
            'window': window}


def sharepoint_excel_job(source: dict, window: dict) -> dict:
    return {'file': source['file'], 'sheet': source['sheet'],
            'site': source.get('site')}


BRIDGES = {
    'api':              (API_BRIDGE_ARN, api_job),
    'redshift':         (REDSHIFT_BRIDGE_ARN, sql_job),
    'postgres':         (POSTGRES_BRIDGE_ARN, sql_job),
    'sharepoint_excel': (SHAREPOINT_READER_ARN, sharepoint_excel_job),
    # 'zeropark' is async - handled by schedule/collect below.
}


def fetch_all_sources(cfg: dict, sources: dict, window: dict) -> dict:
    """Run each source's bridge job; returns {source name: raw key}.
    Zeropark sources are scheduled before this and collected after (see the
    entry point), so their reports build while the other sources download."""
    raw_files = {}
    for name, source in sources.items():
        if source['type'] == 'zeropark':
            continue
        arn, build_job = BRIDGES[source['type']]
        job = {**build_job(source, dates_of(window)),
               'bucket': RAW_BUCKET, 'key': raw_key(cfg, name, window)}
        result = invoke_bridge(arn, source['type'], job)
        raw_files[name] = result['key']
        logger.info(f'Raw {name}: {result.get("rows")} row(s) -> '
                    f's3://{RAW_BUCKET}/{result["key"]}')
    return raw_files


# ---------- Zeropark ----------
# schedule -> (other sources download while the reports build) -> ONE status
# check each -> ready reports download via api-bridge; the rest suspend the
# run (see the docstring).

def schedule_zeropark_reports(cfg: dict, sources: dict, window: dict) -> dict:
    """Fire generate_report for every zeropark source. Returns
    {name: last gateway result} - a result may already be 'success'
    (the gateway itself waits ~75s before answering)."""
    pending = {}
    for name, source in sources.items():
        if source['type'] != 'zeropark':
            continue
        params = {**source['query'], **date_params(window)}
        pending[name] = invoke_bridge(GATEWAY_ARN, 'ZeroparkGateway',
                                      {'action': 'generate_report',
                                       'params': params})
        logger.info(f"Zeropark report '{name}' scheduled "
                    f"(status: {pending[name].get('status')})")
    return pending


def collect_ready_reports(cfg: dict, state: dict) -> None:
    """ONE status check per pending report - no sleep loop. Ready reports
    download to the raw bucket (api-bridge streams the csv_url) and move
    from 'pending' to 'raw_files'; a failed report fails the run."""
    for name, report in list(state['pending'].items()):
        result = (report if report.get('status') == 'success'
                  else invoke_bridge(GATEWAY_ARN, 'ZeroparkGateway',
                                     {'action': 'check_report_status',
                                      'status_url': report['status_url']}))
        if result.get('status') == 'success':
            state['raw_files'][name] = invoke_bridge(
                API_BRIDGE_ARN, 'api-bridge',
                {'url': result['csv_url'],
                 'bucket': RAW_BUCKET,
                 'key': raw_key(cfg, name, state['window'])})['key']
            state['pending'].pop(name)
            logger.info(f'Raw {name} -> s3://{RAW_BUCKET}/{state["raw_files"][name]}')
        elif result.get('status') == 'pending':
            state['pending'][name] = {'status': 'pending',
                                      'status_url': result.get('status_url')
                                      or report.get('status_url')}
        else:
            raise RuntimeError(f"Zeropark report '{name}' failed: {result}")


def date_params(window: dict) -> dict:
    """Run window -> the gateway's date params (date_to is exclusive)."""
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    if window['start'] == window['end'] == yesterday:
        return {'date_range': 'YESTERDAY'}
    end_exclusive = (datetime.strptime(window['end'], '%Y-%m-%d')
                     + timedelta(days=1)).strftime('%Y-%m-%d')
    return {'date_range': 'CUSTOM',
            'date_from': f"{window['start']} 00",
            'date_to': f'{end_exclusive} 00'}


def suspend_run(cfg: dict, state: dict, context) -> dict:
    """End this invocation instead of sleeping; a one-time, self-deleting
    EventBridge schedule re-invokes the engine with the run state."""
    state['attempts'] += 1
    if state['attempts'] > cfg['max_resume_attempts']:
        raise RuntimeError(f'Zeropark report(s) still pending after '
                           f'{cfg["max_resume_attempts"]} resume attempts: '
                           f'{sorted(state["pending"])}')
    create_resume_schedule(cfg, state, context.invoked_function_arn)
    logger.info(f'Suspended - resume {state["attempts"]}/'
                f'{cfg["max_resume_attempts"]} in '
                f'{cfg["resume_delay_minutes"]} min; '
                f'pending: {", ".join(state["pending"])}')
    return {'status': 'suspended', 'pending': sorted(state['pending']),
            'resume_attempt': state['attempts']}


def create_resume_schedule(cfg: dict, state: dict, engine_arn: str) -> None:
    fire_at = (datetime.now(timezone.utc)
               + timedelta(minutes=cfg['resume_delay_minutes']))
    scheduler_client.create_schedule(
        Name=f'{cfg["name"]}-resume-{state["window"]["stamp"]}-{state["attempts"]}',
        ScheduleExpression=f"at({fire_at.strftime('%Y-%m-%dT%H:%M:%S')})",
        ScheduleExpressionTimezone='UTC',
        FlexibleTimeWindow={'Mode': 'OFF'},
        Target={'Arn': engine_arn, 'RoleArn': SCHEDULER_ROLE_ARN,
                'Input': json.dumps({'pipeline': cfg['name'], 'resume': state})},
        ActionAfterCompletion='DELETE',
    )


# =============================================================================
# Step 2: Combine  ──  one duckdb-bridge job runs combine_sql over the raw CSVs
# =============================================================================

def combine_raw_files(cfg: dict, raw_files: dict, window: dict) -> dict:
    """The combine step IS the config's combine_sql, executed by
    duckdb-bridge (which owns the duckdb-layer, validates the {placeholders}
    and renders {start}/{end}). When per-day processed files are wanted, the
    bridge writes them directly in the same job. Returns
    {key, rows, split_keys?} - key is the combined CSV in the raw bucket."""
    job = {
        'sql': cfg['combine_sql'],
        'files': {name: f's3://{RAW_BUCKET}/{key}'
                  for name, key in raw_files.items()},
        'window': dates_of(window),
        'bucket': RAW_BUCKET,
        'key': f'{cfg["name"]}/{window["stamp"]}/_combined.csv',
    }
    processed = cfg['outputs'].get('processed_s3', {})
    if processed.get('enabled') and processed.get('split_by_date'):
        job['split'] = {'bucket': PROCESSED_BUCKET, 'prefix': f'{cfg["name"]}/'}
    return invoke_bridge(DUCKDB_BRIDGE_ARN, 'duckdb-bridge', job)


# =============================================================================
# Step 3: Ship  ──  S3 copies of the combined CSV (no bytes pass through)
# =============================================================================

def ship_outputs(cfg: dict, combined: dict, window: dict) -> dict:
    shipped = {}
    processed = cfg['outputs'].get('processed_s3', {})
    if processed.get('enabled'):
        # split mode: duckdb-bridge already wrote the per-day files
        shipped['processed_keys'] = (
            combined['split_keys'] if processed.get('split_by_date')
            else [copy_s3(combined['key'], PROCESSED_BUCKET,
                          f'{cfg["name"]}/{window["stamp"]}.csv')])
    sharepoint = cfg['outputs'].get('sharepoint_bridge', {})
    if sharepoint.get('enabled'):
        # one 'job' or a 'jobs' list - the same combined CSV can ship to
        # several sheets/files (one uploader job can only target ONE sheet)
        jobs = sharepoint.get('jobs') or [sharepoint['job']]
        shipped['bridge_keys'] = [
            send_to_sharepoint_bridge(cfg, combined, job, n)
            for n, job in enumerate(jobs)]
    return shipped


def send_to_sharepoint_bridge(cfg: dict, combined: dict, job: dict,
                              n: int = 0) -> str:
    """Ship one bridge job: copy the combined CSV into the bridge inbox with
    the job dict as its S3 metadata (the uploader contract). The engine
    fills in the provenance fields - the config never repeats its name.
    n keeps keys distinct when several jobs ship in the same second."""
    job = {**job, 'source-system': 'pipeline-engine', 'source-table': cfg['name']}
    timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d-%H-%M-%S')
    key = f'exports/sharepoint/{cfg["name"]}/{timestamp}-{n}.csv'
    return copy_s3(combined['key'], EXPORT_BUCKET, key,
                   metadata={k: str(v) for k, v in job.items()})


# =============================================================================
# Shared utilities
# =============================================================================

def raw_key(cfg: dict, name: str, window: dict) -> str:
    return f'{cfg["name"]}/{window["stamp"]}/{name}.csv'


def dates_of(window: dict) -> dict:
    return {'start': window['start'], 'end': window['end']}


def copy_s3(source_key: str, bucket: str, key: str,
            metadata: dict | None = None) -> str:
    """Server-side copy of a combined CSV (always from the raw bucket) - the
    engine never downloads data bytes. Combined results are small by design
    (well under copy_object's 5 GB limit)."""
    s3.copy_object(CopySource={'Bucket': RAW_BUCKET, 'Key': source_key},
                   Bucket=bucket, Key=key,
                   ContentType='text/csv',
                   Metadata=metadata or {},
                   MetadataDirective='REPLACE')
    return key


def invoke_bridge(function_arn: str, label: str, payload: dict) -> dict:
    """Synchronous invoke of a bridge/service Lambda; raises the bridge's
    own error message on failure."""
    response = lambda_client.invoke(FunctionName=function_arn,
                                    Payload=json.dumps(payload).encode())
    result = json.loads(response['Payload'].read())
    if response.get('FunctionError'):
        raise RuntimeError(f'{label} failed: {result}')
    return result
