from typing import List, Optional, Dict

from airflow import DAG

from chains.exporter import Exporter
from chains.loader import Loader
from chains.parser import Parser
from constant import get_default_dag_args
from variables import SparkConf, S3Conf


class Blockchain:
    name: str
    exporters: List[Exporter]
    loaders: List[Loader]
    parsers: List[Parser]
    export_schedule_interval: str
    load_schedule_interval: str
    parse_schedule_interval: str
    notification_emails: Optional[List[str]]

    def __init__(
            self,
            name: str,
            exporters: List[Exporter],
            loaders: List[Loader],
            parsers: List[Parser],
            export_schedule_interval: str,
            load_schedule_interval: str,
            parse_schedule_interval: str,
            notification_emails: Optional[List[str]] = None
    ) -> None:
        self.name = name
        self.exporters = exporters
        self.loaders = loaders
        self.parsers = parsers
        self.export_schedule_interval = export_schedule_interval
        self.load_schedule_interval = load_schedule_interval
        self.parse_schedule_interval = parse_schedule_interval
        self.notification_emails = notification_emails

    def build_all_dags(
            self,
            output_bucket: str,
            load_spark_conf: SparkConf,
            parse_spark_conf: SparkConf,
            parse_s3_conf: S3Conf,
            **kwargs
    ) -> List[DAG]:
        return [
            self.build_export_dag(),
            self.build_load_dag(output_bucket, load_spark_conf),
            *self.build_parse_dags(parse_spark_conf, parse_s3_conf),
        ]

    def build_export_dag(self) -> DAG:
        export_dag = DAG(
            dag_id=self.export_dag_name,
            schedule_interval=self.export_schedule_interval,
            default_args=get_default_dag_args(self.notification_emails)
        )

        exporter_map: Dict[str, Exporter] = {}

        for exporter in self.exporters:
            exporter.gen_export_task(export_dag)
            exporter_map[exporter.task_id] = exporter

        for exporter in self.exporters:
            for dependency in exporter.dependencies:
                exporter_map.get(dependency).operator >> exporter_map.get(exporter.task_id).operator

        return export_dag

    def build_load_dag(self, output_bucket: str, spark_conf: SparkConf) -> DAG:
        load_dag = DAG(
            dag_id=self.load_dag_name,
            schedule_interval=self.load_schedule_interval,
            default_args=get_default_dag_args(self.notification_emails)
        )

        loader_map: Dict[str, Loader] = {}

        for loader in self.loaders:
            loader.gen_operators(
                dag=load_dag,
                database=self.database_name,
                temp_database=self.temp_database_name,
                output_bucket=output_bucket,
                spark_conf=spark_conf
            )

            loader_map[loader.resource] = loader

        for loader in self.loaders:
            for enrich_dependency in loader.enrich_dependencies:
                loader_map[enrich_dependency].load_operator >> loader.enrich_operator
            for clean_dependency in loader.clean_dependencies:
                loader_map[clean_dependency].enrich_operator >> loader.clean_operator

        return load_dag

    def build_parse_dags(self, spark_conf: SparkConf, s3_conf: S3Conf) -> List[DAG]:
        parse_dags: List[DAG] = []
        for parser in self.parsers:
            parse_dag = DAG(
                dag_id=parser.dag_id,
                schedule_interval=self.parse_schedule_interval,
                default_args=get_default_dag_args(self.notification_emails)
            )

            parser.gen_operators(dag=parse_dag, spark_conf=spark_conf, s3_conf=s3_conf)
            parse_dags.append(parse_dag)

        return parse_dags

    @property
    def load_dag_name(self) -> str:
        return f'{self.name}_load_dag'

    @property
    def export_dag_name(self) -> str:
        return f'{self.name}_export_dag'

    @property
    def database_name(self) -> str:
        return self.name

    @property
    def temp_database_name(self) -> str:
        return f'{self.name}_raw'
