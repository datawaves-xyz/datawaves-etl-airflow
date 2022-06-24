from typing import List, Optional

from airflow import DAG

from chain.exporter import Exporter
from constant import get_default_dag_args


class Blockchain:
    name: str
    exporters: List[Exporter]
    export_dag: DAG

    def __init__(
            self,
            name: str,
            exporters: List[Exporter],
            exporter_schedule_interval: str,
            notification_emails: Optional[List[str]]
    ) -> None:
        self.name = name
        self.exporters = exporters
        self.export_dag = DAG(
            dag_id=self.export_dag_name,
            schedule_interval=exporter_schedule_interval,
            default_args=get_default_dag_args(notification_emails)
        )

    def _build_exporter_dags(self) -> None:
        operator_map = {export.task_id: export.gen_export_task(self.export_dag) for export in self.exporters}

        for export in self.exporters:
            for dependency in export.dependencies:
                operator_map.get(dependency) >> operator_map.get(export.task_id)

        globals()[self.export_dag_name] = self.export_dag

    def build_all_dags(self) -> None:
        self._build_exporter_dags()

    @property
    def export_dag_name(self) -> str:
        return f'{self.name}_export_dag'
