import os
import pandas as pd
from pathlib import Path
from selectel_api import AsyncObjectStorage
from .archiver import archive_file

BASE_DIR = Path(__file__).resolve().parent.parent
TEMP_DIR = BASE_DIR / "temp"
TEMP_DIR.mkdir(exist_ok=True)

async def process_file(file_path: str, logger):
    try:
        df = pd.read_csv(file_path)
        # Фильтрация: например, только rows с value > 100
        filtered = df[df["value"] > 100]
        target = TEMP_DIR / f"filtered_{Path(file_path).name}"
        filtered.to_csv(target, index=False)
        logger.info(f"File processed and saved: {target}")

        storage = AsyncObjectStorage(
            key_id=os.getenv("STORAGE_KEY_ID"),
            secret=os.getenv("STORAGE_SECRET"),
            endpoint="https://s3.ru-7.storage.selcloud.ru",
            container="de-stepik-training"
        )

        await storage.send_file(str(target))
        logger.info(f"Processed file was uploaded: {target.name}")

        await archive_file(file_path, logger)
        logger.info(f"Source file was archived: {file_path}")

        # Залить лог-файл после каждой обработки
        await storage.send_file(logger.log_path)
        logger.info("Log file loaded to storage")

    except Exception as e:
        logger.error(f"Error while processing {file_path}: {e}")
