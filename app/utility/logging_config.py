import logging
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"

LOG_DIR.mkdir(exist_ok=True)
logger = logging.getLogger(__name__)


def configure_logging(poc_name: str):
    log_path = LOG_DIR / f"{poc_name}.log"

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_path),
        ],
    )
