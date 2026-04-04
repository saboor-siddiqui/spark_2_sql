"""
Repair Agent — attempts to fix invalid SQL using an LLM.

Receives the broken SQL + a list of validation error messages and asks
the LLM to return a corrected SQL string. Limited to Config.max_repair_retries
attempts per query to prevent infinite loops.
"""
from __future__ import annotations

import logging
import re
from typing import Optional

from langchain_core.messages import HumanMessage, SystemMessage

from agents.config import Config, default_config
from agents.llm_parser_agent import _build_llm  # reuse factory

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are a SQL repair specialist. You will be given a broken SQL query and a list of
validation errors. Your job is to return a corrected version of the SQL query.

Rules:
1. Return ONLY the corrected SQL string — no explanation, no markdown fences.
2. Preserve the original intent of the query as closely as possible.
3. Fix syntax errors, missing FROM clauses, and invalid column references.
4. Do not invent columns or tables that were not in the original query.
5. If the SQL is unrepairable, return exactly the word: UNREPAIRABLE
"""


class RepairAgent:
    """
    LLM-based SQL repair agent.

    Uses the same LLM configured in Config. Retries are managed externally
    (by the LangGraph graph loop), but this agent tracks attempt count
    per-instance so it can mark queries as permanently failed.
    """

    def __init__(self, config: Config = default_config):
        self.config = config
        self._llm = None

    @property
    def llm(self):
        if self._llm is None:
            self._llm = _build_llm(self.config)
        return self._llm

    def repair(
        self,
        broken_sql: str,
        errors: list[str],
        attempt: int = 1,
    ) -> Optional[str]:
        """
        Attempt to repair a broken SQL query.

        Args:
            broken_sql: The SQL string that failed validation.
            errors: List of error messages from ValidatorAgent.
            attempt: Current attempt number (for logging).

        Returns:
            Repaired SQL string, or None if repair failed or was marked UNREPAIRABLE.
        """
        if attempt > self.config.max_repair_retries:
            logger.error(
                "RepairAgent: max retries (%d) exceeded. Giving up.",
                self.config.max_repair_retries,
            )
            return None

        error_text = "\n".join(f"  - {e}" for e in errors)
        user_content = (
            f"Broken SQL (attempt {attempt}/{self.config.max_repair_retries}):\n\n"
            f"```sql\n{broken_sql}\n```\n\n"
            f"Validation errors:\n{error_text}\n\n"
            "Return only the corrected SQL."
        )

        messages = [
            SystemMessage(content=_SYSTEM_PROMPT),
            HumanMessage(content=user_content),
        ]

        try:
            response = self.llm.invoke(messages)
        except Exception as exc:
            logger.error("RepairAgent: LLM call failed: %s", exc)
            return None

        raw = response.content if hasattr(response, "content") else str(response)
        raw = re.sub(r"```(?:sql)?", "", raw).strip().strip("`").strip()

        if raw.upper() == "UNREPAIRABLE":
            logger.warning("RepairAgent: LLM declared query UNREPAIRABLE.")
            return None

        logger.info(
            "RepairAgent: attempt %d produced repaired SQL (%d chars).",
            attempt, len(raw)
        )
        return raw
