from dataclasses import dataclass, field
from typing import List, Optional
from abc import ABC, abstractmethod

from airflow import DAG
from airflow.models import BaseOperator


@dataclass
class Exporter(ABC):
    output_bucket: str
    task_id: str
    toggle: bool
    operator: Optional[BaseOperator] = None
    dependencies: List[str] = field(default_factory=list)

    @abstractmethod
    def gen_export_task(self, dag: DAG) -> None:
        pass
