import json
from typing import Optional, Dict, List

from airflow.models import Variable


def read_ethereum_vars(prefix: str, **kwargs) -> Dict[str, any]:
    return {
        'provider_uris': parse_list(read_var('provider_uris', prefix, True, **kwargs)),
        'export_max_workers': parse_int(read_var('export_max_workers', prefix, False, **kwargs)),
        'export_batch_size': parse_int(read_var('export_batch_size', prefix, False, **kwargs)),
        'export_schedule_interval': read_var('export_schedule_interval', prefix, False, **kwargs),
        'notification_emails': parse_list(read_var('notification_emails', prefix, False, **kwargs)),
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
        **read_global_vars(),
    }


def read_global_vars(prefix: Optional[str] = None) -> Dict[str, any]:
    return {'output_bucket': read_var('output_bucket', prefix, True)}


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


def parse_dict(val: Optional[str]) -> Optional[Dict[any, any]]:
    if val is None:
        return None
    return json.loads(val)


def parse_list(val: Optional[str], sep: str = ',') -> Optional[List[str]]:
    if val is None:
        return None
    return [item.strip() for item in val.split(sep)]
