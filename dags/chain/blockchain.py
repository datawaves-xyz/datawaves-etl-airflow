from typing import List, Optional

from airflow import DAG

from chain.exporter import Exporter
from constant import get_default_dag_args


class Blockchain:
    name: str
    exporters: List[Exporter]
    export_schedule_interval: str
    notification_emails: Optional[List[str]]

    def __init__(
            self,
            name: str,
            exporters: List[Exporter],
            export_schedule_interval: str,
            notification_emails: Optional[List[str]]
    ) -> None:
        self.name = name
        self.exporters = exporters
        self.export_schedule_interval = export_schedule_interval
        self.notification_emails = notification_emails

    def build_all_dags(self) -> List[DAG]:
        export_dag = DAG(
            dag_id='ethereum_export_dag',
            schedule_interval=self.export_schedule_interval,
            default_args=get_default_dag_args(self.notification_emails)
        )

        operator_map = {export.task_id: export.gen_export_task(export_dag) for export in self.exporters}

        for export in self.exporters:
            for dependency in export.dependencies:
                operator_map.get(dependency) >> operator_map.get(export.task_id)

        return [export_dag]

    @property
    def export_dag_name(self) -> str:
        return f'{self.name}_export_dag'
