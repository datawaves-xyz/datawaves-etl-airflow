import logging
import os
import boto3


class S3:

    def __init__(self,
                 aws_access_key_id,
                 aws_secret_access_key,
                 region_name,
                 bucket_name: str) -> None:

        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name

        )
        s3 = session.resource('s3')
        self.bucket = s3.Bucket(bucket_name)

    def copy_to_export_path(self, file_path: str, export_path: str) -> None:
        logging.info(f'Calling copy_to_export_path({file_path}, {export_path})')
        filename = os.path.basename(file_path)
        self.bucket.upload_file(file_path, export_path + filename)

    def copy_from_export_path(self, export_path: str, file_path: str) -> None:
        logging.info(f'Calling copy_from_export_path({export_path}, {file_path})')
        filename = os.path.basename(file_path)
        self.bucket.download_file(export_path + filename, file_path)
