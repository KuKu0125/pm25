import os
import logging
import json
from logging.handlers import RotatingFileHandler

_RUN_ID = None

class RunIdFilter(logging.Filter):
    def filter(self, record):
        record.run_id = _RUN_ID or os.getenv("RUN_ID") or "-"
        return True

class JsonFormatter(logging.Formatter):
    def format(self, record):
        base = {
            "ts": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "run_id": getattr(record, "run_id", "-"),
        }
        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(base, ensure_ascii=False)

def set_run_id(run_id: str):
    global _RUN_ID
    _RUN_ID = run_id

def setup_logging(level=logging.INFO, log_dir="logs", log_file="etl.log", json_logs=False, reset=False):
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger()
    if reset:
        for h in list(logger.handlers):
            logger.removeHandler(h)
    elif logger.handlers:
        return logger  

    logger.setLevel(level)

    if json_logs:
        fmt = JsonFormatter(datefmt="%Y-%m-%d %H:%M:%S")
    else:
        fmt = logging.Formatter(
            "%(asctime)s %(levelname)s [%(name)s] run_id=%(run_id)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    run_filter = RunIdFilter()

    file_handler = RotatingFileHandler(
        os.path.join(log_dir, log_file),
        maxBytes=5 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    file_handler.addFilter(run_filter)
    file_handler.setLevel(level)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    console_handler.addFilter(run_filter)
    console_handler.setLevel(level)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger