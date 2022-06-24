import json
from typing import Optional, Dict, List

from airflow.models import Variable


def read_ethereum_vars(prefix: str = 'ethereum') -> Dict[str, any]:
    return {
        'provider_uris': parse_list(read_vars('provider_uris', prefix, True)),
        'export_max_workers': parse_int(read_vars('export_max_workers', prefix, False)),
        'export_batch_size': parse_int(read_vars('export_batch_size', prefix, False)),
        'exporter_schedule_interval': read_vars('exporter_schedule_interval', prefix, False),
        'notification_emails': parse_list(read_vars('notification_emails', prefix, False)),
        'export_blocks_and_transactions_toggle': parse_bool(
            read_vars('export_blocks_and_transactions_toggle', prefix, False, 'True')),
        **read_global_vars(),
    }


def read_global_vars(prefix: Optional[str] = None) -> Dict[str, any]:
    return {'output_bucket': read_vars('output_bucket', prefix, True)}


def read_vars(
        var_name: str,
        var_prefix: Optional[str] = None,
        required: bool = False,
        default: Optional[str] = None,
        **kwargs
) -> str:
    full_var_name = var_name if var_prefix is None else f'{var_prefix}{var_name}'
    var = Variable.get(full_var_name, '')
    if var == '':
        var = None
    if var is None:
        var = default
    if var is None:
        var = kwargs.get(full_var_name)
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
