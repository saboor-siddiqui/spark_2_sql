"""
Java Bridge — thin subprocess wrapper around the compiled Java JAR.

Calls com.dataframe.converter.BridgeCLI which accepts the chain and
tablePrefix as command-line arguments and prints:
    Generated SQL: <sql>

Used by the graph as the "fast path" for simple, well-formed DataFrame chains.
"""
from __future__ import annotations

import logging
import subprocess
from pathlib import Path
from typing import Optional

from agents.config import Config, default_config

logger = logging.getLogger(__name__)

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
            logger.debug("Complex op '%s' detected -> LLM path", op)
            return False
    return True


class JavaBridge:
    """
    Calls the Spark2SQL BridgeCLI Java entry point as a subprocess.

    BridgeCLI accepts two CLI args: the chain string and the table prefix,
    and prints exactly one line: "Generated SQL: <sql>".
    """

    def __init__(self, config: Config = default_config):
        self.config = config
        self._jar = Path(config.java_jar_path)

    def convert(self, chain: str, table_name: str = "") -> Optional[str]:
        """
        Run the Java BridgeCLI for a single chain and return the SQL string,
        or None if conversion failed.
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
            "com.dataframe.converter.BridgeCLI",
            chain,
            self.config.table_prefix,
        ]

        try:
            result = subprocess.run(
                cmd,
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
                "BridgeCLI exited with code %d: %s",
                result.returncode,
                result.stderr[:300],
            )
            return None

        # BridgeCLI prints exactly: "Generated SQL: SELECT ..."
        for line in result.stdout.splitlines():
            if line.startswith("Generated SQL:"):
                sql = line[len("Generated SQL:"):].strip()
                logger.info("Java fast-path produced: %s", sql[:200])
                return sql

        logger.warning(
            "BridgeCLI produced no 'Generated SQL:' line. stdout: %s",
            result.stdout[:300]
        )
        return None
