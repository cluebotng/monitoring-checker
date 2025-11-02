import logging
from pathlib import PosixPath

logger = logging.getLogger(__name__)


async def get_file_based_metrics(base_path: str) -> str:
    lines = []
    root = PosixPath(base_path)
    if root.is_dir():
        for path in root.glob("*.prom"):
            logger.info(f"Loading metrics from {path}")
            with path.open("r") as fh:
                for line in fh.readlines():
                    if not line.startswith("#"):
                        lines.append(line)
    return "".join(lines)
