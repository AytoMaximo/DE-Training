from pathlib import Path

# Для создания асинхронного контекстного менеджера
from contextlib import asynccontextmanager

# Асинхронная версия boto3
from aiobotocore.session import get_session

# Ошибки при обращении к API
from botocore.exceptions import ClientError

class AsyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str):
        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint,
            "verify": False,  # Отключаем проверку SSL-сертификата
        }
        self._bucket = container
        self._session = get_session()

    @asynccontextmanager
    async def _connect(self):
        async with self._session.create_client("s3", **self._auth) as connection:
            yield connection


    async def send_file(self, local_source: str):
       file_ref = Path(local_source)
       target_name = file_ref.name
       async with self._connect() as remote:
          with file_ref.open("rb") as binary_data:
             await remote.put_object(
                 Bucket=self._bucket,
                 Key=target_name,
                 Body=binary_data
             )
             print(f"File {target_name} uploaded successfully.")

    async def fetch_file(self, remote_name: str, local_target: str):
        async with self._connect() as remote:
            response = await remote.get_object(Bucket=self._bucket, Key=remote_name)
            body = await response["Body"].read()
            with open(local_target, "wb") as out:
                out.write(body)
            print(f"File {remote_name} downloaded successfully to {local_target}.")

    async def fetch_file_version(self, remote_name: str, local_target: str, version_id: str):
        async with self._connect() as remote:
            response = await remote.get_object(
                Bucket=self._bucket,
                Key=remote_name,
                VersionId=version_id
            )
            body = await response["Body"].read()
            with open(local_target, "wb") as out:
                out.write(body)
            print(f"File {remote_name} (version {version_id}) downloaded successfully to {local_target}.")

    async def remove_file(self, remote_name: str):
        async with self._connect() as remote:
            await remote.delete_object(Bucket=self._bucket, Key=remote_name)
            print(f"File {remote_name} removed successfully.")

    async def list_files(self):
        async with self._connect() as remote:
            response = await remote.list_objects_v2(Bucket=self._bucket)
            contents = response.get("Contents", [])
            return [obj["Key"] for obj in contents]

    async def file_exists(self, remote_name: str) -> bool:
        async with self._connect() as remote:
            try:
                await remote.head_object(Bucket=self._bucket, Key=remote_name)
                return True
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    return False
                raise

    async def list_file_versions(self, remote_name: str):
        async with self._connect() as remote:
            response = await remote.list_object_versions(Bucket=self._bucket, Prefix=remote_name)
            versions = response.get("Versions", [])
            return [
                {"VersionId": v["VersionId"], "IsLatest": v["IsLatest"], "LastModified": v["LastModified"]}
                for v in versions if v["Key"] == remote_name
            ]