"""
LLM Parser Agent — handles complex or ambiguous Spark DataFrame chains.

Uses an LLM (OpenAI / Anthropic / Google) with a structured few-shot prompt
to produce a validated JSON intermediate representation, then renders it to SQL.

This is the "smart path" — invoked when the Java bridge can't handle the chain.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Optional

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.language_models import BaseChatModel

from agents.config import Config, default_config

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Few-shot examples (chain → structured JSON → SQL)
# ---------------------------------------------------------------------------
_FEW_SHOT_EXAMPLES = """
Example 1 — groupBy + agg + orderBy:
Input chain: df.select("sales.date", "customers.name").join("customers", "sales.customer_id = customers.id").groupBy("date").agg(sum("amount").as("total_sales")).orderBy(desc("total_sales")).limit(10)
Output JSON:
{
  "select": ["date", "customers.name", "SUM(amount) AS total_sales"],
  "from": "sales",
  "joins": [{"type": "INNER", "table": "customers", "on": "sales.customer_id = customers.id"}],
  "where": [],
  "group_by": ["date"],
  "having": [],
  "order_by": ["total_sales DESC"],
  "limit": 10
}

Example 2 — Window function:
Input chain: df.select("employee_id", "salary", "rank().over(Window.partitionBy(\"dept\").orderBy(desc(\"salary\"))).as(\"salary_rank\")")
Output JSON:
{
  "select": ["employee_id", "salary", "RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS salary_rank"],
  "from": "df",
  "joins": [],
  "where": [],
  "group_by": [],
  "having": [],
  "order_by": [],
  "limit": null
}

Example 3 — pivot:
Input chain: df.groupBy("year").pivot("quarter").agg(sum("revenue"))
Output JSON:
{
  "_note": "PIVOT is not standard SQL. Approximated as conditional aggregation.",
  "select": ["year", "SUM(CASE WHEN quarter = 'Q1' THEN revenue END) AS Q1", "SUM(CASE WHEN quarter = 'Q2' THEN revenue END) AS Q2", "SUM(CASE WHEN quarter = 'Q3' THEN revenue END) AS Q3", "SUM(CASE WHEN quarter = 'Q4' THEN revenue END) AS Q4"],
  "from": "df",
  "joins": [],
  "where": [],
  "group_by": ["year"],
  "having": [],
  "order_by": [],
  "limit": null
}
"""

_SYSTEM_PROMPT = f"""You are an expert Spark-to-SQL converter. Given a Spark DataFrame method chain (Scala or PySpark), produce a structured JSON representation of the equivalent SQL query.

Rules:
1. Output ONLY valid JSON — no markdown, no explanation, just the JSON object.
2. The JSON must have exactly these keys: select, from, joins, where, group_by, having, order_by, limit.
3. UDFs and non-SQL operations should be approximated with a SQL equivalent and annotated via a "_note" key.
4. Column names should preserve table qualifiers (e.g. "employees.name").
5. Aggregations go in the "select" array (e.g. "SUM(amount) AS total").
6. Window functions go in the "select" array with full OVER(...) syntax.
7. PIVOT should be approximated using conditional aggregation (CASE WHEN).

{_FEW_SHOT_EXAMPLES}
"""


def _build_llm(config: Config) -> BaseChatModel:
    """Instantiate the correct LangChain chat model based on config."""
    provider = config.llm_provider.lower()

    if provider == "openai":
        from langchain_openai import ChatOpenAI  # type: ignore
        return ChatOpenAI(
            model=config.llm_model,
            api_key=config.openai_api_key or None,
            temperature=0,
        )
    elif provider == "anthropic":
        from langchain_anthropic import ChatAnthropic  # type: ignore
        return ChatAnthropic(
            model=config.llm_model or "claude-3-5-sonnet-20241022",
            api_key=config.anthropic_api_key or None,
            temperature=0,
        )
    elif provider == "google":
        from langchain_google_genai import ChatGoogleGenerativeAI  # type: ignore
        return ChatGoogleGenerativeAI(
            model=config.llm_model or "gemini-1.5-pro",
            google_api_key=config.google_api_key or None,
            temperature=0,
        )
    else:
        raise ValueError(
            f"Unsupported llm_provider '{provider}'. "
            "Set SPARK2SQL_LLM_PROVIDER to: openai, anthropic, or google."
        )


def _json_to_sql(parsed: Dict[str, Any], table_prefix: str) -> str:
    """
    Render the structured JSON intermediate representation to a SQL string.

    This keeps SQL generation in Python (not in the LLM's output) to prevent
    hallucinated SQL syntax.
    """
    parts: List[str] = []

    # SELECT
    select_cols = parsed.get("select") or ["*"]
    parts.append("SELECT " + ", ".join(select_cols))

    # FROM
    from_table = parsed.get("from", "")
    if from_table and not from_table.startswith(table_prefix):
        from_table = table_prefix + from_table
    parts.append(f"FROM `{from_table}`" if from_table else "FROM `unknown_table`")

    # JOINs
    for join in parsed.get("joins") or []:
        join_type = join.get("type", "INNER").upper()
        table = join.get("table", "")
        if table and not table.startswith(table_prefix):
            table = table_prefix + table
        on_clause = join.get("on", "")
        parts.append(f"{join_type} JOIN {table} ON {on_clause}")

    # WHERE
    where_clauses = [c for c in (parsed.get("where") or []) if c]
    if where_clauses:
        parts.append("WHERE " + " AND ".join(where_clauses))

    # GROUP BY
    group_by = [c for c in (parsed.get("group_by") or []) if c]
    if group_by:
        parts.append("GROUP BY " + ", ".join(group_by))

    # HAVING
    having = [c for c in (parsed.get("having") or []) if c]
    if having:
        parts.append("HAVING " + " AND ".join(having))

    # ORDER BY
    order_by = [c for c in (parsed.get("order_by") or []) if c]
    if order_by:
        parts.append("ORDER BY " + ", ".join(order_by))

    # LIMIT
    limit = parsed.get("limit")
    if limit is not None:
        parts.append(f"LIMIT {limit}")

    return "\n".join(parts)


class LLMParserAgent:
    """
    Uses an LLM to parse complex Spark chains into structured SQL.

    The agent produces a JSON IR (not raw SQL) which is then rendered
    deterministically to prevent hallucinated clauses.
    """

    def __init__(self, config: Config = default_config):
        self.config = config
        self._llm: Optional[BaseChatModel] = None  # lazy-loaded

    @property
    def llm(self) -> BaseChatModel:
        if self._llm is None:
            self._llm = _build_llm(self.config)
        return self._llm

    def convert(self, chain: str, variable_name: str = "") -> Optional[str]:
        """
        Convert a complex Spark chain to SQL using the LLM.

        Args:
            chain: The full normalised DataFrame chain string.
            variable_name: Original Scala val name (used as SQL comment).

        Returns:
            SQL string, or None if LLM produced unusable output.
        """
        logger.info("LLMParserAgent: converting chain for '%s'", variable_name or "unknown")

        user_content = (
            f"Convert this Spark DataFrame chain to SQL:\n\n{chain}\n\n"
            "Return ONLY the JSON object, no markdown, no explanation."
        )

        messages = [
            SystemMessage(content=_SYSTEM_PROMPT),
            HumanMessage(content=user_content),
        ]

        try:
            response = self.llm.invoke(messages)
        except Exception as exc:
            logger.error("LLM invocation failed: %s", exc)
            return None

        raw = response.content if hasattr(response, "content") else str(response)

        # Strip accidental markdown fences
        raw = re.sub(r"```(?:json)?", "", raw).strip().strip("`").strip()

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.error("LLM returned invalid JSON: %s\nRaw: %s", exc, raw[:500])
            return None

        sql = _json_to_sql(parsed, self.config.table_prefix)

        # Add a comment tracing back to the original variable name
        if variable_name:
            note = parsed.get("_note", "")
            header = f"-- Source: {variable_name}"
            if note:
                header += f"\n-- Note: {note}"
            sql = header + "\n" + sql

        logger.info("LLMParserAgent produced SQL (%d chars)", len(sql))
        return sql
