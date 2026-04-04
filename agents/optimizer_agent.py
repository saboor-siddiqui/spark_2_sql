"""
Optimizer Agent — post-processes validated SQL for quality and readability.

Transformations (all purely syntactic, using sqlglot):
  1. Normalize JOIN syntax (e.g. add missing JOIN keyword, uppercase type)
  2. Remove redundant WHERE TRUE conditions
  3. Normalize whitespace / keyword casing to UPPERCASE
  4. Append a trailing semicolon
  5. Prepend a source-variable comment if available
"""
from __future__ import annotations

import logging
import re

import sqlglot
import sqlglot.expressions as exp

logger = logging.getLogger(__name__)


class OptimizerAgent:
    """
    Pure-Python post-processor for valid SQL strings.
    Does not call any LLM — all transformations are deterministic.
    """

    def optimize(
        self,
        sql: str,
        variable_name: str = "",
        dialect: str = "bigquery",
    ) -> str:
        """
        Post-process a valid SQL string.

        Args:
            sql: The validated SQL string (may have leading comment lines).
            variable_name: Original Scala val name — used in the header comment.
            dialect: Target SQL dialect for re-serialization.

        Returns:
            Cleaned, normalized SQL string with trailing semicolon.
        """
        # Separate comments from body
        comment_lines = []
        body_lines = []
        for line in sql.splitlines():
            if line.strip().startswith("--"):
                comment_lines.append(line)
            else:
                body_lines.append(line)

        sql_body = "\n".join(body_lines).strip()

        try:
            # Parse → transform → pretty-print
            parsed = sqlglot.parse_one(sql_body, dialect=dialect)
            transformed = self._transform(parsed)
            optimized_body = transformed.sql(dialect=dialect, pretty=True)
        except Exception as exc:
            logger.warning(
                "OptimizerAgent: sqlglot transformation failed (%s). "
                "Returning original SQL with basic cleanup.",
                exc,
            )
            optimized_body = self._basic_cleanup(sql_body)

        # Re-attach comment header
        if variable_name and not any(variable_name in c for c in comment_lines):
            comment_lines.insert(0, f"-- Source variable: {variable_name}")

        header = "\n".join(comment_lines)
        final = (header + "\n" + optimized_body).strip()

        # Ensure trailing semicolon
        if not final.rstrip().endswith(";"):
            final = final.rstrip() + ";"

        logger.debug("OptimizerAgent: optimized SQL for '%s'", variable_name)
        return final

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _transform(self, tree: exp.Expression) -> exp.Expression:
        """Apply AST-level transformations."""
        # Remove WHERE TRUE / WHERE 1=1
        for where in tree.find_all(exp.Where):
            cond = where.this
            if isinstance(cond, exp.Boolean) and cond.this is True:
                where.replace(exp.Where())
            elif (
                isinstance(cond, exp.EQ)
                and isinstance(cond.left, exp.Literal)
                and isinstance(cond.right, exp.Literal)
                and str(cond.left) == "1"
                and str(cond.right) == "1"
            ):
                where.replace(exp.Where())
        return tree

    def _basic_cleanup(self, sql: str) -> str:
        """Fallback cleanup without sqlglot — just normalize spacing."""
        # Uppercase SQL keywords
        keywords = [
            "SELECT", "FROM", "WHERE", "JOIN", "INNER JOIN", "LEFT JOIN",
            "RIGHT JOIN", "GROUP BY", "ORDER BY", "HAVING", "LIMIT",
            "AND", "OR", "ON", "AS", "DISTINCT", "COUNT", "SUM", "AVG",
            "MAX", "MIN", "OVER", "PARTITION BY",
        ]
        for kw in sorted(keywords, key=len, reverse=True):
            sql = re.sub(
                r"\b" + re.escape(kw) + r"\b",
                kw,
                sql,
                flags=re.IGNORECASE,
            )
        return sql
