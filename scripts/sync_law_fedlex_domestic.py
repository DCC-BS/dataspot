import logging
from typing import Any, Dict, Optional

import config
from src.law_fedlex_helpers import sync_fedlex_laws


def sync_law_fedlex_domestic(max_records: Optional[int] = None) -> Dict[str, Any]:
    return sync_fedlex_laws(
        collection_label=config.law_ch_collection_label,
        system_label=config.law_ch_system_label,
        sr_scope="domestic",
        max_records=max_records,
        report_prefix="law_ch_sync_report",
        sync_display_name="LAW CH Sync",
        log_tag="CH",
    )


def main():
    sync_law_fedlex_domestic(max_records=None)


if __name__ == "__main__":
    if config.logging_for_prod:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    logging.info(f"=== CURRENT DATABASE: {config.database_name} ===")
    logging.info(f"Executing {__file__}...")
    main()
