import asyncio

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


    async def remove_file(self, remote_name: str):
        async with self._connect() as remote:
            await remote.delete_object(Bucket=self._bucket, Key=remote_name)
            print(f"File {remote_name} removed successfully.")


async def run_demo():
    storage = AsyncObjectStorage(
        key_id= "2b3e5c8676394f61bb7b61b732f7d3e1",
        secret= "cf5bf11103fc46428333070f882cc12c",
        endpoint="https://s3.ru-7.storage.selcloud.ru",
        container= "de-stepik-training"
    )
    await storage.send_file("sample.txt")
    await storage.fetch_file("sample.txt", "downloaded_sample.txt")
    # await storage.remove_file("sample.txt")


if __name__ == "__main__":
    asyncio.run(run_demo())