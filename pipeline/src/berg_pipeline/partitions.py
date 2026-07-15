import dagster as dg

from berg_pipeline.constants import ARCHIVE_START

# The archive ships monthly ZIPs, so ingest is monthly-partitioned.
monthly_partitions = dg.MonthlyPartitionsDefinition(start_date=ARCHIVE_START)

# Published Parquet is one file per service day.
daily_partitions = dg.DailyPartitionsDefinition(start_date=ARCHIVE_START)
