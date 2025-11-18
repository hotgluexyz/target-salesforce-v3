import importlib
from logging import Logger
from typing import Optional

def real_time_handler(
    config: dict,
    stream_name: str,
    schema_line: str,
    record_line: str,
    logger: Logger,
    input_path: Optional[str] = None,
):
    try:
        mod = importlib.import_module("target_hotglue.lambda")

        if not hasattr(mod, "real_time_handler"):
            raise Exception("This target does not support real time")
    except Exception as e:
        raise

    return mod.real_time_handler(
        config,
        stream_name,
        schema_line,
        record_line,
        logger,
        input_path,
        cli_cmd="target-salesforce-v3",
    )
