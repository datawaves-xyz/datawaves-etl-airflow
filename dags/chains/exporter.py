from dataclasses import dataclass, field
from typing import List, Optional
from abc import ABC, abstractmethod

from airflow import DAG
from airflow.models import BaseOperator


@dataclass
class Exporter(ABC):
    output_bucket: str
    toggle: bool
    task_id: str
    dependencies: List[str]
    operator: Optional[BaseOperator] = field(default=None, init=False)

    @abstractmethod
    def gen_export_task(self, dag: DAG) -> None:
        pass
