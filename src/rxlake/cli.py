from __future__ import annotations

import argparse
import logging
import sys

from rxlake.config import load_settings, parse_month


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rxlake", description=__doc__)
    p.add_argument("--config", help="path to pipeline.yaml")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    for name, help_ in (
        ("ingest", "download a month's CSV into landing"),
        ("bronze", "load landing CSV into the bronze Delta table"),
        ("silver", "clean one month from bronze into silver"),
        ("run", "ingest + bronze + silver + gold for one month"),
    ):
        sp = sub.add_parser(name, help=help_)
        sp.add_argument("month", help="YYYYMM or YYYY-MM")
        if name in ("ingest", "run"):
            sp.add_argument("--force", action="store_true", help="re-download even if present")
        if name == "run":
            sp.add_argument("--skip-download", action="store_true")

    sub.add_parser("gold", help="rebuild all gold tables from silver")
    sub.add_parser("list-months", help="show months published by NHSBSA")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    settings = load_settings(args.config)

    if args.cmd == "list-months":
        from rxlake import ingest

        months = ingest.available_months(ingest.list_resources(settings), settings.resource_prefix)
        print("\n".join(map(str, months)))
        return 0

    if args.cmd == "ingest":
        from rxlake import pipeline

        print(pipeline.run_ingest(settings, parse_month(args.month), args.force))
        return 0

    from rxlake import pipeline
    from rxlake.spark import get_spark

    spark = get_spark(shuffle_partitions=settings.shuffle_partitions)
    try:
        if args.cmd == "bronze":
            pipeline.run_bronze(spark, settings, parse_month(args.month))
        elif args.cmd == "silver":
            print(pipeline.run_silver(spark, settings, parse_month(args.month)))
        elif args.cmd == "gold":
            print(pipeline.run_gold(spark, settings))
        elif args.cmd == "run":
            print(pipeline.run_month(spark, settings, parse_month(args.month), args.skip_download))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
