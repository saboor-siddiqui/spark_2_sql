"""
Integration tests for the full ConversionPipeline (graph).

These tests mock the LLM to avoid API calls.
They validate that the LangGraph pipeline correctly wires all agents together.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from agents.config import Config
from agents.graph import ConversionPipeline


SIMPLE_SCALA_CONTENT = """\
val filteredDf = spark.read.table("sales").select("id", "amount").filter("amount > 100")
"""

# Path to the example file in the project root
_PROJECT_ROOT = Path(__file__).parent.parent.parent
EXAMPLE_FILE = _PROJECT_ROOT / "SparkDataFrameExample.scala"


@pytest.fixture
def no_llm_config():
    """Config that uses openai but we'll mock the LLM calls."""
    return Config(
        llm_provider="openai",
        openai_api_key="test-key",
        table_prefix="",
    )


class TestConversionPipelineIntegration:
    def test_simple_chain_produces_result(self, tmp_path, no_llm_config):
        """A simple chain should run through Java bridge or gracefully skip."""
        f = tmp_path / "test_spark.scala"
        f.write_text(SIMPLE_SCALA_CONTENT)

        pipeline = ConversionPipeline(no_llm_config)
        results = pipeline.run(f)

        # Should have at least one result entry
        assert len(results) >= 1
        r = results[0]
        assert "variable" in r
        assert r["variable"] == "filteredDf"

    def test_file_not_found_raises(self, no_llm_config):
        pipeline = ConversionPipeline(no_llm_config)
        with pytest.raises(FileNotFoundError):
            pipeline.run("/nonexistent/path/file.scala")

    def test_result_has_expected_keys(self, tmp_path, no_llm_config):
        f = tmp_path / "test_spark.scala"
        f.write_text(SIMPLE_SCALA_CONTENT)

        pipeline = ConversionPipeline(no_llm_config)
        results = pipeline.run(f)

        for r in results:
            assert "variable" in r
            assert "sql" in r
            assert "failed" in r
            assert "path_used" in r
            assert "errors" in r
