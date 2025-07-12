import shutil
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
ARCHIVE_DIR = BASE_DIR / "archive"
ARCHIVE_DIR.mkdir(exist_ok=True)

async def archive_file(source_path: str, logger, remove_original: bool = False):
    src = Path(source_path)
    dest = ARCHIVE_DIR / src.name

    if remove_original:
        src.unlink()
        logger.info(f"The source file was deleted: {src}")
    else:
        shutil.move(str(src), str(dest))
        logger.info(f"The source file was archived: {dest}")
