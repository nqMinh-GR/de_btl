"""Spark helpers of the static loader: CSV reading, explicit typing and the GTFS time/date expressions.

* CSVs are read with ``header=true, inferSchema=false`` (every column a string), RFC-4180 quoting
  (``quote='"'``, ``escape='"'``), then header names are BOM/whitespace-stripped and the loader projects
  the columns of :data:`transitpulse.static.validation.FILE_SPECS` (missing optional columns -> null).
* Values are trimmed; blank becomes null. IDs stay strings.
* Numeric/date casts are guarded by regular expressions so a bad value becomes ``null`` (and a quality
  row, computed by comparing the raw and typed columns) instead of a silent truncation (``"1.5"``) or an
  ANSI exception. Dates use ``make_date`` (no datetime-pattern parser, so no legacy-parser exceptions).
* ``service_anchor_expr`` uses ``make_timestamp(y, m, d, 12, 0, 0, tz) - INTERVAL 12 HOURS`` which does not
  depend on the session time zone; the loader still pins ``spark.sql.session.timeZone=UTC``.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from transitpulse.static.gtfs_time import GTFS_TIME_PATTERN
from transitpulse.static.validation import FileSpec, normalize_header

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import Column, DataFrame, SparkSession

__all__ = [
    "GTFS_TIME_REGEX",
    "YYYYMMDD_REGEX",
    "INT_REGEX",
    "DOUBLE_REGEX",
    "read_gtfs_csv",
    "project",
    "empty_frame",
    "clean",
    "int_col",
    "double_col",
    "date_col",
    "flag_col",
    "gtfs_seconds_col",
    "service_anchor_expr",
    "quote_ident",
    "sql_string_literal",
]

GTFS_TIME_REGEX = GTFS_TIME_PATTERN
YYYYMMDD_REGEX = r"^[0-9]{8}$"
INT_REGEX = r"^[+-]?[0-9]{1,9}$"
DOUBLE_REGEX = r"^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$"


def quote_ident(name: str) -> str:
    """Back-quote a column name for Spark (handles dots/spaces)."""
    return "`" + name.replace("`", "``") + "`"


def sql_string_literal(value: str) -> str:
    """Single-quoted Spark SQL string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def read_gtfs_csv(spark: SparkSession, path: Path) -> DataFrame:
    """Read one GTFS CSV with every column as string and normalized header names."""
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("encoding", "UTF-8")
        .csv(str(path))
    )
    return df.toDF(*normalize_header(df.columns))


def clean(name: str) -> Column:
    """Trimmed string value of a column; blank -> null."""
    from pyspark.sql import functions as F

    trimmed = F.trim(F.col(quote_ident(name)))
    return F.when(trimmed == "", F.lit(None).cast("string")).otherwise(trimmed)


def project(df: DataFrame, spec: FileSpec) -> DataFrame:
    """Select the spec columns (trimmed strings); absent optional columns are added as null strings."""
    from pyspark.sql import functions as F

    present = set(df.columns)
    cols = [clean(c).alias(c) if c in present else F.lit(None).cast("string").alias(c) for c in spec.columns]
    return df.select(*cols)


def empty_frame(spark: SparkSession, spec: FileSpec) -> DataFrame:
    """Zero-row frame with the spec columns as strings (missing optional file)."""
    from pyspark.sql import types as T

    schema = T.StructType([T.StructField(c, T.StringType(), True) for c in spec.columns])
    return spark.createDataFrame([], schema)


def int_col(c: Column) -> Column:
    """Integer value of a trimmed string column; anything but an optionally signed integer -> null."""
    from pyspark.sql import functions as F

    return F.when(F.coalesce(c.rlike(INT_REGEX), F.lit(False)), c.cast("int"))


def double_col(c: Column) -> Column:
    """Double value of a trimmed string column; non-numeric text (incl. NaN/Infinity) -> null."""
    from pyspark.sql import functions as F

    return F.when(F.coalesce(c.rlike(DOUBLE_REGEX), F.lit(False)), c.cast("double"))


def date_col(c: Column) -> Column:
    """GTFS ``YYYYMMDD`` -> DateType; malformed or impossible dates -> null (``make_date``, non-ANSI)."""
    from pyspark.sql import functions as F

    ok = F.coalesce(c.rlike(YYYYMMDD_REGEX), F.lit(False))
    return F.when(
        ok, F.make_date(c.substr(1, 4).cast("int"), c.substr(5, 2).cast("int"), c.substr(7, 2).cast("int"))
    )


def flag_col(c: Column) -> Column:
    """GTFS 0/1 flag -> boolean; other values -> null."""
    from pyspark.sql import functions as F

    return F.when(c == "1", F.lit(True)).when(c == "0", F.lit(False))


def gtfs_seconds_col(c: Column) -> Column:
    """``H:MM:SS`` (hours may exceed 23) -> int seconds after the service-day anchor; null/invalid -> null."""
    from pyspark.sql import functions as F

    ok = F.coalesce(c.rlike(GTFS_TIME_REGEX), F.lit(False))
    h = F.regexp_extract(c, GTFS_TIME_REGEX, 1).cast("int")
    m = F.regexp_extract(c, GTFS_TIME_REGEX, 2).cast("int")
    s = F.regexp_extract(c, GTFS_TIME_REGEX, 3).cast("int")
    return F.when(ok, h * 3600 + m * 60 + s)


def service_anchor_expr(date_column: str, agency_timezone: str) -> Column:
    """``UTC(local noon of the service date in agency_timezone) - 12 hours`` as a TimestampType column."""
    from pyspark.sql import functions as F

    d = quote_ident(date_column)
    tz = sql_string_literal(agency_timezone)
    return F.expr(f"make_timestamp(year({d}), month({d}), day({d}), 12, 0, 0, {tz}) - INTERVAL 12 HOURS")
