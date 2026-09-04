import logging
from typing import Any, Dict, Optional

import config
from scripts.sync_law_ch import sync_fedlex_laws


def sync_law_ch_international(max_records: Optional[int] = None) -> Dict[str, Any]:
    return sync_fedlex_laws(
        collection_label=config.law_ch_intl_collection_label,
        system_label=config.law_ch_intl_system_label,
        sr_scope="international",
        max_records=max_records,
        report_prefix="law_ch_intl_sync_report",
        sync_display_name="LAW CH International Sync",
        log_tag="CH Intl",
    )


def main():
    sync_law_ch_international(max_records=None)

    # Use this for initial import
    #for i in [500, 1000, 1500, 2000, 2500, 3000, 3500, 4000]:
    #    sync_law_ch_international(max_records=i)


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
