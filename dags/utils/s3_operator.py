import logging
import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3Operator:
    hook: S3Hook
    bucket: str

    def __init__(self, aws_conn_id: str, bucket: str) -> None:
        self.hook = S3Hook(aws_conn_id=aws_conn_id)
        self.bucket = bucket

    def copy_to_export_path(self, file_path: str, export_path: str) -> None:
        logging.info(f'Calling copy_to_export_path({file_path}, {export_path})')
        filename = os.path.basename(file_path)
        self.hook.load_file(
            filename=file_path,
            bucket_name=self.bucket,
            key=export_path + filename,
            replace=True,
            encrypt=False
        )

    def copy_from_export_path(self, export_path: str, file_path: str) -> None:
        logging.info(f'Calling copy_from_export_path({export_path}, {file_path})')
        filename = os.path.basename(file_path)
        self.hook.download_file(
            bucket=self.bucket,
            key=export_path + filename,
            local_path=file_path
        )
