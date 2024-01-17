import logging
from typing import Dict, Optional

LOGGER = logging.getLogger(__name__)

LABEL = "text stream processor"


def process(
    data: str,
    *,
    file_metadata: Optional[Dict] = None,
    arg1: Optional[str] = None,
    arg2: Optional[str] = None,
    **_kwargs,
) -> None:
    print(f"in example text stream processor got data {data}")
    print(f"got arg1 {arg1} and arg2 {arg2}")
