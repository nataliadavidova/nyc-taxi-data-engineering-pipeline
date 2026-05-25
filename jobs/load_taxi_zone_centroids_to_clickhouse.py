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

Important architectural note:
    This script is a setup/reference-data loader, not a monthly taxi fact job.
    It is intentionally not part of the main monthly Airflow pipeline because
    taxi zone centroid coordinates are static reference data.

Production-like improvements:
1. The CSV path is resolved relative to the project root.
   Why:
   - Path("data/geo/...") works only if the script is launched from project root;
   - resolving from __file__ makes the script safer and reproducible.

2. The script validates loaded data, not only prints query results.
   Why:
   - a setup job should fail if the lookup table is incomplete or corrupted;
   - Superset geospatial charts depend on this table.

3. The script no longer imports execute_clickhouse_query from
   truncate_clickhouse_gold_tables.py.
   Why:
   - importing a helper from a truncate job is confusing;
   - this script now has its own small ClickHouse HTTP helper.
   - later we can move common ClickHouse helpers into jobs/clickhouse_utils.py.

4. The table is loaded with truncate + reload.
   Why:
   - this is a full static lookup snapshot;
   - running the script multiple times should not create duplicates.
"""

from base64 import b64encode
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    validate_config,
)


# Project root:
# this file is expected to live in jobs/, therefore parents[1] is the project root.
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Use an absolute path derived from the script location.
# This is safer than Path("data/geo/..."), because it works even if the script
# is launched from another working directory.
CSV_PATH = PROJECT_ROOT / "data" / "geo" / "taxi_zone_centroids.csv"

# Expected number of NYC Taxi zones in the prepared centroid file.
# This value is used as an actual validation check after loading.
EXPECTED_ROWS_COUNT = 263

# Very broad sanity bounds for NYC coordinates.
# They are intentionally not too tight, but they help catch swapped columns,
# wrong files, or invalid coordinate extraction.
MIN_EXPECTED_LATITUDE = 40.0
MAX_EXPECTED_LATITUDE = 41.0
MIN_EXPECTED_LONGITUDE = -75.0
MAX_EXPECTED_LONGITUDE = -72.0

# Some NYC Taxi zones may be represented by multiple geometry parts.
# Therefore the prepared centroid file can contain several rows for the same
# location_id. Superset virtual datasets deduplicate this table before joining
# it with demand metrics, so these duplicates are allowed but monitored.
MAX_EXPECTED_DUPLICATE_LOCATION_IDS_COUNT = 3


def get_target_table() -> str:
    """
    Return the full ClickHouse table name.

    We use CLICKHOUSE_DATABASE from config.py to stay consistent with the rest
    of the project and avoid hardcoding the database name in multiple files.
    """

    return f"{CLICKHOUSE_DATABASE}.taxi_zone_centroids"


def get_clickhouse_url() -> str:
    """
    Build ClickHouse HTTP URL.

    This script uses ClickHouse HTTP interface because it loads a tiny CSV file
    and runs simple validation queries. Spark is not needed here.
    """

    query_params = urlencode({"database": CLICKHOUSE_DATABASE})
    return f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?{query_params}"


def execute_clickhouse_query(query: str, print_response: bool = True) -> str:
    """
    Execute a ClickHouse query through the HTTP interface and return response text.

    print_response=False is useful for helper queries where we parse the result
    and print a clearer custom message afterwards.
    """

    validate_config()

    request = Request(
        url=get_clickhouse_url(),
        data=query.encode("utf-8"),
        method="POST",
    )

    auth_token = b64encode(
        f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD or ''}".encode("utf-8")
    ).decode("ascii")

    request.add_header("Authorization", f"Basic {auth_token}")

    try:
        with urlopen(request, timeout=120) as response:
            response_text = response.read().decode("utf-8").strip()

            if response_text and print_response:
                print(response_text)

            return response_text

    except HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"ClickHouse HTTP error {error.code}: {error_body}"
        ) from error

    except URLError as error:
        raise RuntimeError(f"Cannot connect to ClickHouse: {error}") from error


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

    if not csv_path.is_file():
        raise ValueError(f"CSV path is not a file: {csv_path}")

    print(f"Input CSV found: {csv_path}")


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
    text. Otherwise Python may add extra whitespace after the last CSV row, and
    ClickHouse can try to parse that whitespace as one more CSV row.
    """

    csv_text = csv_path.read_text(encoding="utf-8")

    # Remove trailing whitespace/newlines from the file content, then add
    # exactly one final newline. This prevents ClickHouse from seeing an
    # extra blank row after the last real CSV row.
    csv_text = csv_text.strip() + "\n"

    query = (
        f"INSERT INTO {target_table}\n"
        "FORMAT CSVWithNames\n"
        f"{csv_text}"
    )

    print(f"Loading CSV into ClickHouse table: {target_table}")
    print(f"CSV path: {csv_path}")

    execute_clickhouse_query(query)


def read_single_int(query: str) -> int:
    """
    Execute a ClickHouse query that returns a single integer value.

    FORMAT TabSeparated is used to make parsing predictable.
    """

    response_text = execute_clickhouse_query(query, print_response=False).strip()

    if not response_text:
        raise ValueError("ClickHouse query returned an empty response")

    return int(response_text.splitlines()[0].strip())


def validate_rows_count(target_table: str) -> None:
    """
    Validate that the loaded table has the expected number of rows.

    This turns EXPECTED_ROWS_COUNT from a comment-like constant into a real
    quality gate.
    """

    query = f"""
    SELECT count()
    FROM {target_table}
    FORMAT TabSeparated
    """

    rows_count = read_single_int(query)

    print(f"Loaded rows count: {rows_count}")

    if rows_count != EXPECTED_ROWS_COUNT:
        raise ValueError(
            f"Unexpected rows count in {target_table}: "
            f"expected {EXPECTED_ROWS_COUNT}, got {rows_count}"
        )


def validate_location_id_duplicates(target_table: str) -> None:
    """
    Check duplicate location_id values.

    Important:
    Some taxi zones are represented by multiple geometry parts, so the centroid
    lookup may contain duplicate location_id values.

    This is allowed for the raw/reference centroid table, but it must be
    visible and controlled because joining this table directly to demand metrics
    can duplicate metrics.

    Superset virtual datasets deduplicate centroids to one row per location_id
    before joining with pickup/dropoff demand.
    """

    query = f"""
    SELECT
        count() - uniqExact(location_id) AS duplicate_location_ids_count
    FROM {target_table}
    FORMAT TabSeparated
    """

    duplicate_location_ids_count = read_single_int(query)

    print(f"Duplicate location_id count: {duplicate_location_ids_count}")

    if duplicate_location_ids_count > MAX_EXPECTED_DUPLICATE_LOCATION_IDS_COUNT:
        raise ValueError(
            f"{target_table} contains too many duplicated location_id values: "
            f"{duplicate_location_ids_count}. "
            f"Expected at most {MAX_EXPECTED_DUPLICATE_LOCATION_IDS_COUNT}."
        )

    if duplicate_location_ids_count > 0:
        print(
            "WARNING: Duplicate location_id values found. "
            "This is expected for multi-part taxi zones. "
            "Do not join this table directly to demand metrics without "
            "deduplicating location_id first."
        )

def validate_required_text_fields(target_table: str) -> None:
    """
    Validate that required text fields are not empty.

    borough and zone are used in dashboard labels and joins, so empty values
    would make Superset maps and tooltips less useful.
    """

    query = f"""
    SELECT
        countIf(trim(borough) = '') AS empty_borough_count,
        countIf(trim(zone) = '') AS empty_zone_count
    FROM {target_table}
    FORMAT TabSeparated
    """

    response_text = execute_clickhouse_query(query, print_response=False).strip()
    empty_borough_count, empty_zone_count = [
        int(value) for value in response_text.split("\t")
    ]

    print(f"Empty borough count: {empty_borough_count}")
    print(f"Empty zone count: {empty_zone_count}")

    if empty_borough_count > 0:
        raise ValueError(
            f"{target_table} contains {empty_borough_count} rows "
            "with empty borough"
        )

    if empty_zone_count > 0:
        raise ValueError(
            f"{target_table} contains {empty_zone_count} rows with empty zone"
        )


def validate_coordinate_ranges(target_table: str) -> None:
    """
    Validate basic latitude/longitude ranges.

    This helps catch:
    - wrong file loaded;
    - swapped latitude/longitude columns;
    - malformed centroid extraction.
    """

    query = f"""
    SELECT
        min(latitude) AS min_latitude,
        max(latitude) AS max_latitude,
        min(longitude) AS min_longitude,
        max(longitude) AS max_longitude
    FROM {target_table}
    FORMAT TabSeparated
    """

    response_text = execute_clickhouse_query(query, print_response=False).strip()
    min_latitude, max_latitude, min_longitude, max_longitude = [
        float(value) for value in response_text.split("\t")
    ]

    print(f"Latitude range: {min_latitude} → {max_latitude}")
    print(f"Longitude range: {min_longitude} → {max_longitude}")

    if min_latitude < MIN_EXPECTED_LATITUDE or max_latitude > MAX_EXPECTED_LATITUDE:
        raise ValueError(
            f"Latitude range looks invalid for {target_table}: "
            f"{min_latitude} → {max_latitude}"
        )

    if (
        min_longitude < MIN_EXPECTED_LONGITUDE
        or max_longitude > MAX_EXPECTED_LONGITUDE
    ):
        raise ValueError(
            f"Longitude range looks invalid for {target_table}: "
            f"{min_longitude} → {max_longitude}"
        )


def preview_loaded_data(target_table: str) -> None:
    """
    Print a small preview for observability.

    This is safe because the table has only 263 rows.
    """

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


def check_loaded_data(target_table: str) -> None:
    """
    Run sanity checks after loading.

    These checks now fail the job if something is wrong, instead of only
    printing query results.
    """

    validate_rows_count(target_table)
    validate_location_id_duplicates(target_table)
    validate_required_text_fields(target_table)
    validate_coordinate_ranges(target_table)
    preview_loaded_data(target_table)


def main() -> None:
    """
    Main loading flow.

    Execution order:
    1. Validate source CSV.
    2. Create ClickHouse table.
    3. Truncate old data.
    4. Load CSV.
    5. Run actual validation checks.
    """

    validate_config()

    target_table = get_target_table()

    validate_input_file(CSV_PATH)
    create_table(target_table)
    truncate_table(target_table)
    load_csv_to_clickhouse(CSV_PATH, target_table)
    check_loaded_data(target_table)

    print("Taxi zone centroids loaded to ClickHouse successfully")


if __name__ == "__main__":
    main()