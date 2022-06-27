import logging
from datetime import timedelta, datetime
from typing import List, Callable, Optional

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.python import PythonOperator


class Exporter:
    resource: List[str]
    toggle: bool
    provider_uris: List[str]
    export_callable: Callable
    dependencies: List[str]
    operator: Optional[BaseOperator]

    def __init__(
            self,
            task_id: str,
            toggle: bool,
            export_callable: Callable,
            dependencies: List[str] = None
    ) -> None:
        self.task_id = task_id
        self.toggle = toggle
        self.export_callable = export_callable
        self.dependencies = [] if dependencies is None else dependencies

    def get_fallback_callable(self, provider_uris: List[str]) -> Callable:
        def python_callable_with_fallback(**kwargs):
            for index, provider_uri in enumerate(provider_uris):
                kwargs['provider_uri'] = provider_uri
                try:
                    self.export_callable(**kwargs)
                    break
                except Exception as e:
                    if index < (len(provider_uris) - 1):
                        logging.exception('An exception occurred. Trying another uri')
                    else:
                        raise e

        return python_callable_with_fallback

    def gen_export_task(self, dag: DAG, provider_uris: List[str]) -> None:
        if self.toggle:
            self.operator = PythonOperator(
                task_id=self.task_id,
                python_callable=self.get_fallback_callable(provider_uris),
                provide_context=True,
                execution_timeout=timedelta(hours=15),
                dag=dag
            )
        else:
            self.operator = None

    @staticmethod
    def export_folder_path(directory: str, date: datetime) -> str:
        return f'export/{directory}/block_date={date.strftime("%Y-%m-%d")}/'
