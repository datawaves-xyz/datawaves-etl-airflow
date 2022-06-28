import base64
import logging
from datetime import datetime
from typing import Union, List, Optional

import requests
import yaml
from mashumaro import DataClassDictMixin


class Token(DataClassDictMixin):
    def __init__(
            self,
            name: str,
            id: str,
            symbol: str,
            decimals: int,
            address: Union[int, str],
            end: Optional[datetime] = None
    ) -> None:
        self.name = name
        self.id = id
        self.decimals = decimals
        self.symbol = symbol
        self.end = end

        if address is not None:
            if isinstance(address, str):
                self.address = address
            elif isinstance(address, int):
                self.address = hex(address)
            else:
                raise ValueError('The type of address must be str or int.')


class TokenProvider:
    def get_tokens(self) -> List[Token]:
        raise NotImplementedError()


class DuneTokenProvider(TokenProvider):
    def get_tokens(self) -> List[Token]:
        yaml_file_path = "https://api.github.com/repos/duneanalytics/abstractions/contents/prices/ethereum/coinpaprika.yaml"
        res = requests.get(yaml_file_path)
        yaml_content = base64.b64decode(res.json()['content']).decode('utf-8')

        tokens = []
        for item in yaml.safe_load(yaml_content):
            try:
                tokens.append(Token.from_dict(item))
            except Exception:
                logging.warning(f'The item dose not contain ever attribute: {item}')

        return tokens
