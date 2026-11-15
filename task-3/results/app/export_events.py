"""Stream shipment_events from PostgreSQL to a CSV file."""

import csv
import os
import sys
from pathlib import Path

import psycopg


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def main() -> int:
    output_dir = Path(env("EXPORT_DIR", "/export"))
    output_name = env("EXPORT_FILENAME", "shipment_events.csv")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / output_name
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    connect_kwargs = {
        "host": env("DB_HOST", "localhost"),
        "port": env("DB_PORT", "5432"),
        "dbname": env("DB_NAME", "logistics"),
        "user": env("DB_USER", "logistics"),
        "password": env("DB_PASSWORD", "local-only-password"),
        "connect_timeout": int(env("DB_CONNECT_TIMEOUT", "10")),
    }

    try:
        with psycopg.connect(**connect_kwargs) as connection:
            # A named cursor keeps the result server-side and avoids loading the
            # potentially million-row table into container memory.
            with connection.cursor(name="shipment-events-export") as cursor:
                cursor.execute(
                    "SELECT event_id, shipment_id, event_type, event_time, location "
                    "FROM shipment_events ORDER BY event_id"
                )
                with temp_path.open("w", encoding="utf-8", newline="") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(["event_id", "shipment_id", "event_type", "event_time", "location"])
                    for row in cursor:
                        writer.writerow(row)
        temp_path.replace(output_path)
        print(f"Exported shipment_events to {output_path}")
        return 0
    except Exception as exc:  # noqa: BLE001 - CLI must turn all failures into non-zero exit.
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        print(f"Export failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
