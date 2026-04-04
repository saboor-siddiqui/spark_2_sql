"""
Central configuration for the Spark2SQL LangGraph agent pipeline.

Override via environment variables or by passing a Config object directly.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Resolve project root (one level up from this agents/ directory)
# ---------------------------------------------------------------------------
_AGENTS_DIR = Path(__file__).parent
_PROJECT_ROOT = _AGENTS_DIR.parent


@dataclass
class Config:
    # ---- Table prefix injected into every FROM / JOIN clause ---------------
    table_prefix: str = field(
        default_factory=lambda: os.getenv("SPARK2SQL_TABLE_PREFIX", "axp-lumid.dw_anon.")
    )

    # ---- LLM settings ------------------------------------------------------
    llm_provider: str = field(
        default_factory=lambda: os.getenv("SPARK2SQL_LLM_PROVIDER", "openai")
    )
    llm_model: str = field(
        default_factory=lambda: os.getenv("SPARK2SQL_LLM_MODEL", "gpt-4o")
    )
    openai_api_key: str = field(
        default_factory=lambda: os.getenv("OPENAI_API_KEY", "")
    )
    anthropic_api_key: str = field(
        default_factory=lambda: os.getenv("ANTHROPIC_API_KEY", "")
    )
    google_api_key: str = field(
        default_factory=lambda: os.getenv("GOOGLE_API_KEY", "")
    )

    # ---- Java JAR path -----------------------------------------------------
    java_jar_path: Path = field(
        default_factory=lambda: _PROJECT_ROOT
        / "target"
        / "spark-2-sql-converter-1.0-SNAPSHOT-jar-with-dependencies.jar"
    )

    # ---- Repair loop guard -------------------------------------------------
    max_repair_retries: int = 3

    # ---- Output ------------------------------------------------------------
    output_path: Path = field(
        default_factory=lambda: _PROJECT_ROOT / "output.sql"
    )

    def validate(self) -> None:
        """Raise ValueError for missing critical settings."""
        if self.llm_provider == "openai" and not self.openai_api_key:
            raise ValueError(
                "OPENAI_API_KEY is not set. "
                "Export it or set SPARK2SQL_LLM_PROVIDER to another provider."
            )
        if self.llm_provider == "anthropic" and not self.anthropic_api_key:
            raise ValueError("ANTHROPIC_API_KEY is not set.")
        if self.llm_provider == "google" and not self.google_api_key:
            raise ValueError("GOOGLE_API_KEY is not set.")


# Singleton default config (can be overridden in tests or CLI)
default_config = Config()
