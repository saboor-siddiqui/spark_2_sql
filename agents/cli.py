"""
CLI entry point for the Spark2SQL LangGraph agent pipeline.

Usage:
    python -m agents.cli convert path/to/SparkFile.scala
    python -m agents.cli convert path/to/SparkFile.scala --output output.sql
    python -m agents.cli convert path/to/SparkFile.scala --dry-run
    python -m agents.cli convert path/to/SparkFile.scala --llm anthropic --table-prefix "mydb."
"""
from __future__ import annotations

import argparse
import datetime
import logging
import sys
from pathlib import Path

from agents.config import Config
from agents.graph import ConversionPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="spark2sql",
        description="Convert Apache Spark DataFrame chains to SQL using a LangGraph agent pipeline.",
    )
    sub = p.add_subparsers(dest="command", required=True)

    convert = sub.add_parser("convert", help="Convert a Spark file to SQL.")
    convert.add_argument("file", type=Path, help="Path to the .scala Spark file.")
    convert.add_argument(
        "--output", "-o", type=Path, default=None,
        help="Path to write the output .sql file (default: output.sql next to input file).",
    )
    convert.add_argument(
        "--dry-run", action="store_true",
        help="Print SQL to stdout instead of writing to a file.",
    )
    convert.add_argument(
        "--llm", default="openai", choices=["openai", "anthropic", "google"],
        help="LLM provider for the semantic parser and repair agents.",
    )
    convert.add_argument(
        "--model", default=None,
        help="Specific model name (e.g. gpt-4o, claude-3-5-sonnet-20241022).",
    )
    convert.add_argument(
        "--table-prefix", default=None,
        help="Table prefix prepended to every FROM/JOIN (e.g. 'mydb.schema.').",
    )
    convert.add_argument(
        "--dialect", default="bigquery",
        choices=["bigquery", "spark", "ansi", "mysql", "postgres", "snowflake"],
        help="SQL dialect for validation and pretty-printing.",
    )
    return p


def _run_convert(args: argparse.Namespace) -> int:
    # Build config
    cfg = Config(llm_provider=args.llm)
    if args.model:
        cfg.llm_model = args.model
    if args.table_prefix:
        cfg.table_prefix = args.table_prefix

    # Validate config early (will raise if API key missing)
    try:
        cfg.validate()
    except ValueError as e:
        logger.error("%s", e)
        return 1

    # Run pipeline
    pipeline = ConversionPipeline(cfg)
    logger.info("Starting conversion of '%s' …", args.file)

    try:
        results = pipeline.run(args.file)
    except FileNotFoundError as e:
        logger.error("%s", e)
        return 1

    if not results:
        logger.warning("No operations extracted from '%s'.", args.file)
        return 0

    # Assemble output
    now = datetime.datetime.now().isoformat(timespec="seconds")
    lines = [
        "-- ================================================================",
        f"-- Generated SQL Queries",
        f"-- Source:    {args.file}",
        f"-- Generated: {now}",
        f"-- Provider:  {cfg.llm_provider} / {cfg.llm_model}",
        "-- ================================================================",
        "",
    ]

    successes = 0
    failures = 0
    for r in results:
        var = r.get("variable", "unknown")
        sql = r.get("sql")
        failed = r.get("failed", False)
        path = r.get("path_used", "?")
        errors = r.get("errors", [])

        if sql and not failed:
            successes += 1
            lines.append(f"-- [{path.upper()} path] {var}")
            lines.append(sql)
            lines.append("")
        else:
            failures += 1
            lines.append(f"-- [FAILED] {var}")
            for err in errors:
                lines.append(f"--   {err}")
            if sql:
                lines.append(f"-- Best-effort SQL (may be invalid):")
                for sql_line in (sql or "").splitlines():
                    lines.append(f"-- {sql_line}")
            lines.append("")

    output_text = "\n".join(lines)

    logger.info(
        "Conversion complete: %d succeeded, %d failed.", successes, failures
    )

    if args.dry_run:
        print(output_text)
        return 0

    # Determine output path
    out_path: Path = args.output or args.file.with_suffix(".sql")
    out_path.write_text(output_text, encoding="utf-8")
    logger.info("SQL written to '%s'.", out_path)
    return 0


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "convert":
        sys.exit(_run_convert(args))
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
