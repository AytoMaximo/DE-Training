from watchfiles import awatch
import asyncio
from .processor import process_file

async def watch_directory(path: str, logger):
    async for changes in awatch(path):
        for change, file_path in changes:
            if change.name == "added" and file_path.endswith(".csv"):
                logger.info(f"New file was detected: {file_path}")
                asyncio.create_task(process_file(file_path, logger))
