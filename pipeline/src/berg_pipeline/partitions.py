from datetime import date, timedelta

import dagster as dg

from berg_pipeline.constants import ARCHIVE_START

# The archive ships monthly ZIPs, so ingest is monthly-partitioned.
monthly_partitions = dg.MonthlyPartitionsDefinition(start_date=ARCHIVE_START)

# Published Parquet is keyed by UTC departure day. The first service day's 00:xx local trains
# depart on the preceding UTC date, so the daily range begins one day before archive service.
daily_partitions = dg.DailyPartitionsDefinition(
    start_date=(date.fromisoformat(ARCHIVE_START) - timedelta(days=1)).isoformat()
)
