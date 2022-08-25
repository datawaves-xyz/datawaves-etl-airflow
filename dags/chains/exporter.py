from dataclasses import dataclass
from typing import List, Optional
from abc import ABC, abstractmethod

from airflow import DAG
from airflow.models import BaseOperator


@dataclass
class ExporterMixIn:
    output_bucket: str
    task_id: str
    toggle: bool
    dependencies: List[str]


class Exporter(ABC):
    operator: Optional[BaseOperator] = None

    @abstractmethod
    def gen_export_task(self, dag: DAG) -> None:
        pass
