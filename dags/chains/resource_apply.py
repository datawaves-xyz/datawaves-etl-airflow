from dataclasses import dataclass
from typing import Optional, List, Dict

from chains.transfer_client import TransferRawTable, TransferABI


@dataclass(frozen=True)
class ABIElement:
    chain: str
    table_name: Optional[str] = None
    dataset_name: Optional[str] = None
    contract_name: Optional[str] = None


@dataclass(frozen=True)
class SparkResource:
    executor_cores: int = 1
    executor_memory: int = 1
    executor_instances: int = 1
    driver_cores: int = 1
    driver_memory: int = 1

    def __dict__(self) -> Dict[str, str]:
        return {
            "spark.executor.cores": str(self.executor_cores),
            "spark.executor.memory": f"{self.executor_memory}g",
            "spark.executor.instances": str(self.executor_instances),
            "spark.driver.cores": str(self.driver_cores),
            "spark.driver.memory": f"{self.driver_memory}g"
        }


@dataclass(frozen=True)
class TasksResource:
    tasks: List[ABIElement]
    resource: SparkResource


transfer_tasks_resource = [
    TasksResource(
        tasks=[
            ABIElement(chain='ethereum', table_name='traces'),
            ABIElement(chain='ethereum', table_name='logs')
        ],
        resource=SparkResource(
            executor_cores=3,
            executor_memory=16,
            executor_instances=4,
            driver_cores=2,
            driver_memory=4
        )
    )
]


def get_raw_table_spark_resource(raw_table: TransferRawTable) -> Optional[Dict[str, str]]:
    for resource in transfer_tasks_resource:
        for task in resource.tasks:
            if task.chain == raw_table.chain and task.table_name == raw_table.table:
                return resource.resource.__dict__()
    return None


def get_abi_table_spark_resource(abi: TransferABI) -> Optional[Dict[str, str]]:
    for resource in transfer_tasks_resource:
        for task in resource.tasks:
            if task.chain == abi.chain and \
                    task.dataset_name == abi.dataset_name and \
                    task.contract_name == abi.contract_name:
                return resource.resource.__dict__()
    return None
