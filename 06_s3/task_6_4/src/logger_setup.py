import logging
from logging.handlers import RotatingFileHandler

def get_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("csv_pipeline")
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=2)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(module)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.log_path = log_path  # сохраняем путь как атрибут
    return logger