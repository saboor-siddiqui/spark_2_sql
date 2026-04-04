"""
Extractor Agent — Phase 1 of the conversion pipeline.

Responsibilities:
  - Read a Scala Spark file
  - Remove comments and normalize whitespace
  - Extract each `val <name> = <df>.<chain>` block as an independent operation string
  - Resolve simple variable dependencies (val df2 = df1.filter(...) → inlines df1's chain)
  - Return a list of ExtractedOperation dicts for the parser agent
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ExtractedOperation:
    """One DataFrame chain extracted from the source file."""
    variable_name: str          # Scala val name, e.g. "collibraPreHeaderIcmv"
    source_df: str              # The root DataFrame variable, e.g. "spark"
    chain: str                  # Normalised chain string, e.g. "spark.read.table(...).select(...)"
    source_line: int            # Approximate line number in the original file
    raw: str                    # Original un-normalised text (for debugging/LLM context)


class ExtractorAgent:
    """
    Extracts individual Spark DataFrame operation chains from a Scala source file.

    Improvements over the Java DataFrameCodeExtractor:
      - Appends sentinel newline to fix regex dropping last val block
      - Inlines known variable chains (e.g. val df2 = df1.filter(...) becomes
        <df1's chain>.filter(...))
      - Strips both single-line (//) and multi-line (/* */) comments
    """

    _VAL_PATTERN = re.compile(
        # Captures: val <name> = <source>.<operation-chain>
        r"val\s+(\w+)\s*=\s*(\w+)\.((?:[^\n]*?\n?\s*\.?)*?)(?=\s*val\s|\s*$)",
        re.MULTILINE | re.DOTALL,
    )
    _LEADING_WS = re.compile(r"(?m)^\s+")
    _MULTILINE_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
    _SINGLELINE_COMMENT = re.compile(r"//[^\n]*")

    def run(self, file_path: str | Path) -> List[ExtractedOperation]:
        """
        Parse the given file and return a list of ExtractedOperation objects.

        Args:
            file_path: Path to the .scala Spark file.

        Returns:
            List of ExtractedOperation, one per val assignment found.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file is empty.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")

        raw_content = path.read_text(encoding="utf-8")
        if not raw_content.strip():
            raise ValueError(f"Input file is empty: {path}")

        logger.info("ExtractorAgent: processing '%s' (%d chars)", path, len(raw_content))
        return self._extract(raw_content)

    def run_from_string(self, content: str) -> List[ExtractedOperation]:
        """Convenience method for testing — accepts raw Scala content as a string."""
        if not content.strip():
            raise ValueError("Content string is empty")
        return self._extract(content)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _normalise(self, content: str) -> str:
        """Strip comments and collapse extraneous whitespace."""
        content = self._multiline_comment.sub("", content)
        content = self._SINGLELINE_COMMENT.sub("", content)
        content = self._LEADING_WS.sub("", content)
        content = content.strip()
        # FIX: Append sentinel newline so (?=\s*val|\s*$) matches the last block
        return content + "\n"

    # Make comment patterns accessible from _normalise via lowercase alias
    _multiline_comment = _MULTILINE_COMMENT

    def _clean_chain(self, raw_chain: str) -> str:
        """Normalise a raw operation chain into a single-line string."""
        return (
            raw_chain
            .replace("\r\n", "\n")
            .replace("\r", "\n")
            # Dots surrounded by optional whitespace/newlines → plain dot
            .replace("\n.", ".")
            # Collapse remaining newlines + leading spaces
            .replace("\n", " ")
            # spaces around dots
            .replace(" . ", ".")
            .replace(". ", ".")
            .replace(" .", ".")
            # Normalise spaces
            .replace("  ", " ")
            .strip()
        )

    def _line_of(self, content: str, match_start: int) -> int:
        """Return 1-indexed line number of a character offset."""
        return content[:match_start].count("\n") + 1

    def _extract(self, content: str) -> List[ExtractedOperation]:
        # Track known variable → chain for dependency inlining
        known_vars: Dict[str, str] = {}
        ops: List[ExtractedOperation] = []

        normalised = self._normalise(content)

        for match in self._VAL_PATTERN.finditer(normalised):
            var_name   = match.group(1)   # e.g. "collibraPreHeaderIcmv"
            source_var = match.group(2)   # e.g. "spark" or "df"
            raw_chain  = match.group(3)   # e.g. "read.table(\"sales\").select(...)"
            line_no    = self._line_of(normalised, match.start())

            cleaned = self._clean_chain(raw_chain)

            # Dependency inlining: if source_var is a previously seen val,
            # prepend its chain so the parser sees the full lineage.
            if source_var in known_vars:
                full_chain = known_vars[source_var] + "." + cleaned
                logger.debug(
                    "Inlining '%s' chain into '%s'", source_var, var_name
                )
            else:
                full_chain = source_var + "." + cleaned

            known_vars[var_name] = full_chain

            op = ExtractedOperation(
                variable_name=var_name,
                source_df=source_var,
                chain=full_chain,
                source_line=line_no,
                raw=match.group(0),
            )
            logger.info(
                "Extracted op '%s' (line %d): %s", var_name, line_no,
                full_chain[:120] + ("..." if len(full_chain) > 120 else "")
            )
            ops.append(op)

        if not ops:
            logger.warning("No DataFrame val assignments found in the provided content.")

        return ops
