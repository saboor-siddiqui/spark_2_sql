"""Tests for JavaBridge routing logic (is_simple_chain)."""
import pytest
from agents.java_bridge import is_simple_chain


class TestIsSimpleChain:
    def test_simple_select_filter(self):
        chain = 'df.select("name", "age").filter("age > 30")'
        assert is_simple_chain(chain) is True

    def test_simple_groupby_count(self):
        chain = 'df.groupBy("id").count()'
        assert is_simple_chain(chain) is True

    def test_simple_join(self):
        chain = 'df.select("a.id").join("b", "a.id = b.id")'
        assert is_simple_chain(chain) is True

    def test_complex_window(self):
        chain = 'df.select("id", "rank().over(Window.partitionBy(\\"dept\\"))")'
        assert is_simple_chain(chain) is False

    def test_complex_pivot(self):
        chain = 'df.groupBy("year").pivot("quarter").agg(sum("revenue"))'
        assert is_simple_chain(chain) is False

    def test_complex_explode(self):
        chain = 'df.select(explode(col("tags")))'
        assert is_simple_chain(chain) is False
