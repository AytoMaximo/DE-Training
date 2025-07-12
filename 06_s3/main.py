import asyncio
import os
from selectel_api import AsyncObjectStorage
from dotenv import load_dotenv

load_dotenv()

storage = AsyncObjectStorage(
    key_id=os.getenv("STORAGE_KEY_ID"),
    secret=os.getenv("STORAGE_SECRET"),
    endpoint="https://s3.ru-7.storage.selcloud.ru",
    container="de-stepik-training"
)

async def run_demo():

    # await storage.send_file("sample.txt")
    # await storage.fetch_file("sample.txt", "downloaded_sample.txt")
    # await storage.remove_file("sample.txt")

    print(f"Files list: {await storage.list_files()}")

    file_to_find = "sample.txt"
    file_to_find_invalid = "invalid.txt"
    # print(f"File {file_to_find}: {await storage.file_exists(file_to_find)}")
    # print(f"File {file_to_find_invalid}: {await storage.file_exists(file_to_find_invalid)}")

    print(f"File versions for {file_to_find}: {await storage.list_file_versions(file_to_find)}")
    await storage.fetch_file_version(
        remote_name=file_to_find,
        local_target="downloaded_sample_version.txt",
        version_id="1752311629626507004"
    )

if __name__ == "__main__":
    asyncio.run(run_demo())