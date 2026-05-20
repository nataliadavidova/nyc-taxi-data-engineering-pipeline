"""
Load taxi zone centroid coordinates into ClickHouse.

This script loads a small geographical lookup table prepared from NYC Taxi Zone
geometry.

Input file:
    data/geo/taxi_zone_centroids.csv

Expected CSV columns:
    location_id
    borough
    zone
    longitude
    latitude

Target ClickHouse table:
    nyc_taxi.taxi_zone_centroids

Why this table is needed:
    Superset map charts need latitude and longitude.
    Our gold marts contain taxi zone IDs and names, but they do not contain
    coordinates. This lookup table allows us to join taxi demand with map points.
"""

from pathlib import Path

from config import CLICKHOUSE_DATABASE
from truncate_clickhouse_gold_tables import execute_clickhouse_query


# We keep the path relative to the project root.
# Inside the Airflow container the project root is usually /opt/airflow.
CSV_PATH = Path("data/geo/taxi_zone_centroids.csv")

# Expected number of NYC Taxi zones in the downloaded geometry file.
# We use it as a simple sanity check after loading.
EXPECTED_ROWS_COUNT = 263


def get_target_table() -> str:
    """
    Return the full ClickHouse table name.

    We use CLICKHOUSE_DATABASE from config.py to stay consistent with the rest
    of the project and avoid hardcoding the database name in multiple files.
    """
    return f"{CLICKHOUSE_DATABASE}.taxi_zone_centroids"


def validate_input_file(csv_path: Path) -> None:
    """
    Check that the input CSV exists before trying to load it.

    This prevents a confusing ClickHouse error later and gives us a clear
    message if the file is missing or the path is wrong.
    """
    if not csv_path.exists():
        raise FileNotFoundError(
            f"CSV file not found: {csv_path}. "
            "Expected file: data/geo/taxi_zone_centroids.csv"
        )


def create_table(target_table: str) -> None:
    """
    Create the ClickHouse table for taxi zone centroids.

    Column meaning:
    - location_id: taxi zone ID from TLC lookup / geometry files.
    - borough: NYC borough name.
    - zone: readable taxi zone name.
    - longitude: map X coordinate.
    - latitude: map Y coordinate.

    MergeTree is enough here because this is a tiny lookup table.
    ORDER BY location_id makes lookups and joins predictable.
    """
    query = f"""
    CREATE TABLE IF NOT EXISTS {target_table}
    (
        location_id UInt16,
        borough String,
        zone String,
        longitude Float64,
        latitude Float64
    )
    ENGINE = MergeTree()
    ORDER BY location_id
    """

    print("Creating ClickHouse table if not exists:")
    print(query)

    execute_clickhouse_query(query)


def truncate_table(target_table: str) -> None:
    """
    Clear the target table before reload.

    The source CSV is a full lookup snapshot, not an incremental feed.
    Therefore truncate + reload is simple and idempotent:
    running the script twice will not create duplicate rows.
    """
    query = f"TRUNCATE TABLE {target_table}"

    print("Truncating ClickHouse table before reload:")
    print(query)

    execute_clickhouse_query(query)


def load_csv_to_clickhouse(csv_path: Path, target_table: str) -> None:
    """
    Load the CSV file into ClickHouse using FORMAT CSVWithNames.

    Why CSVWithNames:
    - our CSV has a header row;
    - ClickHouse can parse CSV values itself;
    - we do not need to manually escape quotes, commas, or special characters
      in zone names.

    Important:
    For INSERT ... FORMAT CSVWithNames everything after FORMAT is treated as
    data, not as regular SQL.

    Therefore we must not use an indented triple-quoted f-string after the CSV
    text. Otherwise Python adds extra whitespace after the last CSV row, and
    ClickHouse tries to parse that whitespace as one more CSV row.
    """
    # Read CSV as plain text.
    csv_text = csv_path.read_text(encoding="utf-8")

    # Remove trailing whitespace/newlines from the file content, then add
    # exactly one final newline. This prevents ClickHouse from seeing an
    # extra blank row after the last real CSV row.
    csv_text = csv_text.strip() + "\n"

    # Build the INSERT query without any extra indentation after csv_text.
    # Everything after FORMAT CSVWithNames is input data.
    query = (
        f"INSERT INTO {target_table}\n"
        "FORMAT CSVWithNames\n"
        f"{csv_text}"
    )

    print(f"Loading CSV into ClickHouse table: {target_table}")
    print(f"CSV path: {csv_path}")

    execute_clickhouse_query(query)


def check_loaded_data(target_table: str) -> None:
    """
    Run simple sanity checks after loading.

    Expected:
    - 263 rows;
    - latitude around 40.x;
    - longitude around -73/-74.

    These checks help us catch cases where the wrong file was loaded.
    """
    count_query = f"""
    SELECT count()
    FROM {target_table}
    """

    print("Checking rows count:")
    execute_clickhouse_query(count_query)

    coordinates_query = f"""
    SELECT
        count() AS rows_count,
        min(latitude) AS min_latitude,
        max(latitude) AS max_latitude,
        min(longitude) AS min_longitude,
        max(longitude) AS max_longitude
    FROM {target_table}
    """

    print("Checking coordinates range:")
    execute_clickhouse_query(coordinates_query)

    preview_query = f"""
    SELECT
        location_id,
        borough,
        zone,
        longitude,
        latitude
    FROM {target_table}
    ORDER BY location_id
    LIMIT 10
    """

    print("Preview:")
    execute_clickhouse_query(preview_query)


def main() -> None:
    """
    Main loading flow.

    Execution order:
    1. Validate source CSV.
    2. Create ClickHouse table.
    3. Truncate old data.
    4. Load CSV.
    5. Check loaded data.
    """
    target_table = get_target_table()

    validate_input_file(CSV_PATH)
    create_table(target_table)
    truncate_table(target_table)
    load_csv_to_clickhouse(CSV_PATH, target_table)
    check_loaded_data(target_table)

    print("Taxi zone centroids loaded to ClickHouse successfully")


if __name__ == "__main__":
    main()