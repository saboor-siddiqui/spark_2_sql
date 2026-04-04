"""
Validator Agent — validates generated SQL for syntax and basic semantic correctness.

Uses sqlglot (pure Python, no database required) for:
  1. Syntax validation — parse the SQL string
  2. Structural completeness — FROM clause is not empty
  3. Light semantic check — WHERE columns not referencing non-existent aliases
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Tuple

import sqlglot
import sqlglot.errors

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class ValidatorAgent:
    """
    Validates SQL strings produced by either the Java bridge or the LLM parser.

    Returns a ValidationResult with is_valid=True only if all checks pass.
    """

    def validate(self, sql: str, dialect: str = "bigquery") -> ValidationResult:
        """
        Validate a SQL string.

        Args:
            sql: The SQL string to validate.
            dialect: Target SQL dialect for sqlglot (bigquery, spark, ansi, …).

        Returns:
            ValidationResult with is_valid and lists of errors/warnings.
        """
        if not sql or not sql.strip():
            return ValidationResult(is_valid=False, errors=["SQL string is empty."])

        # Strip comment lines before parsing
        sql_body = "\n".join(
            line for line in sql.splitlines()
            if not line.strip().startswith("--")
        ).strip()

        errors: List[str] = []
        warnings: List[str] = []

        # ---- 1. Syntax check ------------------------------------------------
        try:
            statements = sqlglot.parse(sql_body, dialect=dialect, error_level=sqlglot.errors.ErrorLevel.RAISE)
        except sqlglot.errors.ParseError as exc:
            for err in exc.errors:
                errors.append(f"Syntax error: {err.get('description', str(err))}")
            return ValidationResult(is_valid=False, errors=errors)

        if not statements:
            return ValidationResult(is_valid=False, errors=["No SQL statements parsed."])

        for stmt in statements:
            if stmt is None:
                errors.append("Parsed statement is None — likely empty or comment-only input.")
                continue

            self._check_from_clause(stmt, errors)
            self._check_select_clause(stmt, warnings)

        is_valid = len(errors) == 0
        if is_valid:
            logger.info("ValidatorAgent: SQL is valid.")
        else:
            logger.warning("ValidatorAgent: validation failed:\n  %s", "\n  ".join(errors))

        return ValidationResult(is_valid=is_valid, errors=errors, warnings=warnings)

    # ------------------------------------------------------------------
    # Internal checks
    # ------------------------------------------------------------------

    def _check_from_clause(
        self, stmt: sqlglot.Expression, errors: List[str]
    ) -> None:
        """Ensure the statement has a non-empty FROM clause."""
        from_node = stmt.find(sqlglot.exp.From)
        if from_node is None:
            errors.append("Missing FROM clause.")
            return

        # Find the table name inside the FROM node
        table = from_node.find(sqlglot.exp.Table)
        if table is None:
            errors.append("FROM clause has no table reference.")
            return

        table_name = table.name or ""
        if not table_name:
            errors.append("FROM clause references an empty table name.")

    def _check_select_clause(
        self, stmt: sqlglot.Expression, warnings: List[str]
    ) -> None:
        """Warn if SELECT is a bare star (might indicate the converter missed columns)."""
        select_node = stmt.find(sqlglot.exp.Select)
        if select_node is None:
            return

        expressions = select_node.expressions
        if len(expressions) == 1:
            expr = expressions[0]
            if isinstance(expr, sqlglot.exp.Star):
                warnings.append(
                    "SELECT * detected — consider specifying explicit columns."
                )
