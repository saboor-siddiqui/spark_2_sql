"""Tests for ExtractorAgent."""
import pytest
from agents.extractor_agent import ExtractorAgent


SIMPLE_SCALA = """
val df = spark.read.table("sales")
val filtered = df.select("name", "age").filter("age > 30")
"""

MULTILINE_SCALA = """
val collibraPreHeaderIcmv = spark.read.table("collibra_header_icmv")
  .select("offer_nm_incentive", "incentive_buckets", "collibra_model_id")
  .withColumnRenamed("offr_total_cost_am", "total_cost")
  .filter("crc_channel_cost > 1000")
"""

SINGLE_VAL_NO_TRAILING_NEWLINE = "val df = spark.read.table(\"sales\").select(\"id\")"


@pytest.fixture
def extractor():
    return ExtractorAgent()


class TestExtractorAgent:
    def test_simple_extraction(self, extractor):
        ops = extractor.run_from_string(SIMPLE_SCALA)
        assert len(ops) == 2
        names = [o.variable_name for o in ops]
        assert "df" in names
        assert "filtered" in names

    def test_multiline_chain_normalised_to_single_line(self, extractor):
        ops = extractor.run_from_string(MULTILINE_SCALA)
        assert len(ops) == 1
        chain = ops[0].chain
        # Should be a single line with no embedded newlines
        assert "\n" not in chain
        assert "collibra_header_icmv" in chain
        assert "withColumnRenamed" in chain

    def test_last_val_block_not_dropped(self, extractor):
        """
        Regression: the regex EOF fix must capture the LAST val block
        even when there is no trailing newline.
        """
        ops = extractor.run_from_string(SINGLE_VAL_NO_TRAILING_NEWLINE)
        assert len(ops) == 1
        assert ops[0].variable_name == "df"

    def test_variable_dependency_inlining(self, extractor):
        """
        val df2 = df1.filter(...) should inline df1's chain into df2.
        """
        scala = """
val df1 = spark.read.table("orders").select("id", "amount")
val df2 = df1.filter("amount > 100")
"""
        ops = extractor.run_from_string(scala)
        assert len(ops) == 2
        df2_op = next(o for o in ops if o.variable_name == "df2")
        # df2's chain should contain the full inlined chain from df1
        assert "read.table" in df2_op.chain

    def test_comment_stripping(self, extractor):
        scala = """
// This is a comment
val df = spark.read.table("sales") /* inline comment */ .select("id")
"""
        ops = extractor.run_from_string(scala)
        assert len(ops) == 1
        assert "//" not in ops[0].chain

    def test_empty_content_raises(self, extractor):
        with pytest.raises(ValueError, match="empty"):
            extractor.run_from_string("   ")

    def test_no_val_assignments_returns_empty(self, extractor):
        ops = extractor.run_from_string("val x = 1 + 2")
        # No DataFrame .method() chains — should return empty
        assert ops == []
