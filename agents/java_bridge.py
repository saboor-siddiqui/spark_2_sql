"""
Java Bridge — thin subprocess wrapper around the compiled Java JAR.

Calls DataFrameToSQLConverter via the JAR's main class. Used by the
parser_agent as the "fast path" for simple, well-formed DataFrame chains.
"""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Optional

from agents.config import Config, default_config

logger = logging.getLogger(__name__)

# Operations the Java converter handles reliably
_SUPPORTED_OPS = {
    "select", "filter", "where", "join", "groupBy",
    "orderBy", "distinct", "limit", "withColumn", "withColumnRenamed",
    "count",
}

# Operations that require the LLM path
_COMPLEX_OPS = {
    "pivot", "explode", "flatMap", "mapPartitions",
    "Window", "partitionBy", "rowNumber", "rank", "dense_rank",
    "crossJoin", "broadcast",
}


def is_simple_chain(chain: str) -> bool:
    """
    Return True if the chain contains only operations the Java JAR supports reliably.

    Strategy: if any complex-op keyword appears in the chain, route to LLM.
    """
    for op in _COMPLEX_OPS:
        if op in chain:
            logger.debug("Complex op '%s' detected → LLM path", op)
            return False
    return True


class JavaBridge:
    """
    Calls the Spark2SQL Java JAR as a subprocess.

    The JAR's DataFrameToSQLConverter.main() is NOT designed for subprocess
    invocation with an argument, so we use a small stdin-based protocol:
    we pass the chain on the command line and capture stdout.

    For now we invoke via `java -cp <jar> com.dataframe.converter.DataFrameToSQLConverter`
    and rely on the existing main() demo output.  The bridge is intentionally
    simple — the LLM path handles everything the JAR cannot.
    """

    def __init__(self, config: Config = default_config):
        self.config = config
        self._jar = Path(config.java_jar_path)

    def convert(self, chain: str, table_name: str = "") -> Optional[str]:
        """
        Run the Java converter for a single chain and return the SQL string,
        or None if conversion failed.

        Args:
            chain: Full normalised chain string, e.g. "spark.read.table(...).select(...)"
            table_name: Optional override for the table name; empty means let the Java
                        converter infer it from the chain.

        Returns:
            SQL string, or None on failure.
        """
        if not self._jar.exists():
            logger.warning(
                "Java JAR not found at %s — skipping fast path. "
                "Run `mvn package` to build it.",
                self._jar,
            )
            return None

        cmd = [
            "java",
            "-cp", str(self._jar),
            "com.dataframe.converter.DataFrameToSQLConverter",
        ]

        try:
            # Pass the chain + table_name via stdin (pipe) so we don't have
            # to restructure the Java main() — we simply grep its stdout for
            # "Generated SQL: " lines.
            result = subprocess.run(
                cmd,
                input=f"{chain}\n{table_name}\n",
                capture_output=True,
                text=True,
                timeout=15,
            )
        except FileNotFoundError:
            logger.error("'java' executable not found. Is JRE installed?")
            return None
        except subprocess.TimeoutExpired:
            logger.error("Java converter timed out for chain: %s", chain[:120])
            return None

        if result.returncode != 0:
            logger.warning(
                "Java converter exited with code %d: %s",
                result.returncode,
                result.stderr[:300],
            )
            return None

        # The Java main prints lines like: "Generated SQL: SELECT ..."
        for line in result.stdout.splitlines():
            if line.startswith("Generated SQL:"):
                sql = line[len("Generated SQL:"):].strip()
                logger.info("Java fast-path produced: %s", sql[:200])
                return sql

        logger.warning(
            "Java converter produced no 'Generated SQL:' line. "
            "stdout: %s", result.stdout[:300]
        )
        return None
