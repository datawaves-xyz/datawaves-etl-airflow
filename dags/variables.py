import json
from dataclasses import dataclass
from typing import Optional, Dict, List, Any

from airflow.models import Variable
from mashumaro import DataClassDictMixin


@dataclass
class SparkConf(DataClassDictMixin):
    java_class: str
    conf: Dict[str, Any]
    jars: str
    application: str
    application_args: List[str]


@dataclass
class S3Conf(DataClassDictMixin):
    access_key: str
    secret_key: str
    bucket: str
    region: str


def read_evm_vars(prefix: str, **kwargs) -> Dict[str, Any]:
    return {
        # Export
        'provider_uris': parse_list(read_var('provider_uris', prefix, True, **kwargs)),
        'export_max_workers': parse_int(read_var('export_max_workers', prefix, False, **kwargs)),
        'export_batch_size': parse_int(read_var('export_batch_size', prefix, False, **kwargs)),
        'export_schedule_interval': read_var('export_schedule_interval', prefix, False, **kwargs),
        'export_daofork_traces_option': parse_bool(
            read_var('export_daofork_traces_option', prefix, False, **kwargs)),
        'export_genesis_traces_option': parse_bool(
            read_var('export_genesis_traces_option', prefix, False, **kwargs)),
        'export_blocks_and_transactions_toggle': parse_bool(
            read_var('export_blocks_and_transactions_toggle', prefix, False, **kwargs)),
        'export_receipts_and_logs_toggle': parse_bool(
            read_var('export_receipts_and_logs_toggle', prefix, False, **kwargs)),
        'extract_token_transfers_toggle': parse_bool(
            read_var('extract_token_transfers_toggle', prefix, False, **kwargs)),
        'export_traces_toggle': parse_bool(
            read_var('export_traces_toggle', prefix, False, **kwargs)),
        'extract_contracts_toggle': parse_bool(
            read_var('extract_contracts_toggle', prefix, False, **kwargs)),
        'extract_tokens_toggle': parse_bool(
            read_var('extract_tokens_toggle', prefix, False, **kwargs)),
        'export_prices_toggle': parse_bool(
            read_var('export_prices_toggle', prefix, False, **kwargs)),
        # Load
        'load_schedule_interval': read_var('load_schedule_interval', prefix, False, **kwargs),
        'load_spark_conf': read_individual_spark_vars(prefix + 'loader_'),
        # Verify
        'verify_schedule_interval': read_var('verify_schedule_interval', prefix, False, **kwargs),
        'verify_spark_conf': read_individual_spark_vars(prefix + 'verifier_'),
        # Parse
        'parse_schedule_interval': read_var('parse_schedule_interval', prefix, False, **kwargs),
        'parse_spark_conf': read_individual_spark_vars(prefix + 'parser_'),
        'parse_s3_conf': read_global_s3_vars(),
        # Experiment_Parse
        'experiment_parse_schedule_interval': read_var('experiment_parse_schedule_interval', prefix, False, **kwargs),
        'experiment_parse_spark_conf': read_individual_spark_vars(prefix + 'experiment_parser_'),
        # Common
        'notification_emails': parse_list(read_var('notification_emails', prefix, False, **kwargs)),
        **read_global_vars(),
    }


def read_transfer_vars(prefix: str = 'transfer_', **kwargs) -> Dict[str, Any]:
    return {
        'spark_conf': read_individual_spark_vars(prefix),
        'schema_registry_s3_conf': read_global_s3_vars(),
        'schedule_interval': read_var('schedule_interval', prefix, False, **kwargs),
        'notification_emails': parse_list(read_var('notification_emails', prefix, False, **kwargs)),
        **read_global_vars()
    }


def read_individual_spark_vars(prefix: str, **kwargs) -> SparkConf:
    global_spark_vars = read_global_spark_vars()
    global_spark_vars['java_class'] = read_var('spark_java_class', prefix, True)

    load_spark_conf = parse_dict(read_var('spark_conf', prefix, False))
    if load_spark_conf is not None:
        global_spark_vars['conf'].update(load_spark_conf)

    loader_spark_args = parse_list(read_var('spark_application_args', prefix, False))
    global_spark_vars['application_args'] = loader_spark_args if loader_spark_args is not None else []

    individual_spark_jars = read_var('spark_jars', prefix, False)
    global_spark_vars['jars'] = individual_spark_jars if individual_spark_jars != '' else global_spark_vars['jars']

    individual_spark_application = read_var('spark_application', prefix, False)
    global_spark_vars['application'] = individual_spark_application \
        if individual_spark_application != '' else global_spark_vars['application']

    return SparkConf.from_dict(global_spark_vars)


def read_global_vars(prefix: Optional[str] = None) -> Dict[str, Any]:
    return {
        'output_bucket': read_var('output_bucket', prefix, True),
        'coinpaprika_auth_key': read_var('coinpaprika_auth_key', prefix, True)
    }


def read_global_spark_vars(prefix: Optional[str] = None) -> Dict[str, Any]:
    return {
        'jars': read_var('spark_jars', prefix, True),
        'application': read_var('spark_application', prefix, True),
        'conf': parse_dict(read_var('spark_conf', prefix, True))
    }


def read_global_s3_vars(prefix: Optional[str] = None) -> S3Conf:
    return S3Conf.from_dict(parse_dict(read_var('s3_conf', prefix, True)))


def read_var(
        var_name: str,
        var_prefix: Optional[str] = None,
        required: bool = False,
        **kwargs
) -> Optional[str]:
    full_var_name = var_name if var_prefix is None else f'{var_prefix}{var_name}'
    var = Variable.get(full_var_name, '')
    if var == '':
        var = None
    if var is None:
        var = kwargs.get(var_name)
    if required and var is None:
        raise ValueError(f'{full_var_name} variable is required.')
    return var


def parse_bool(bool_string: Optional[str], default: bool = True) -> bool:
    if isinstance(bool_string, bool):
        return bool_string
    if bool_string is None or len(bool_string) == 0:
        return default
    else:
        return bool_string.lower() in ['true', 'yes']


def parse_int(val: Optional[str]) -> Optional[int]:
    if val is None:
        return None
    return int(val)


def parse_dict(val: Optional[str]) -> Optional[Dict[Any, Any]]:
    if val is None:
        return None
    return json.loads(val)


def parse_list(val: Optional[str], sep: str = ',') -> Optional[List[str]]:
    if val is None:
        return None
    return [item.strip() for item in val.split(sep)]
