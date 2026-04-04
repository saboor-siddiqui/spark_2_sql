"""Tests for ValidatorAgent."""
import pytest
from agents.validator_agent import ValidatorAgent


@pytest.fixture
def validator():
    return ValidatorAgent()


VALID_SQL = "SELECT name, age FROM `mydb.schema.users` WHERE age > 30"
VALID_WITH_JOIN = (
    "SELECT u.name, o.amount FROM `mydb.schema.users` AS u "
    "INNER JOIN `mydb.schema.orders` AS o ON u.id = o.user_id"
)
MISSING_FROM = "SELECT name, age"
EMPTY_SQL = ""
COMMENT_ONLY = "-- This is just a comment"


class TestValidatorAgent:
    def test_valid_simple_sql(self, validator):
        result = validator.validate(VALID_SQL)
        assert result.is_valid
        assert result.errors == []

    def test_valid_join_sql(self, validator):
        result = validator.validate(VALID_WITH_JOIN)
        assert result.is_valid

    def test_missing_from_clause(self, validator):
        result = validator.validate(MISSING_FROM)
        assert not result.is_valid
        assert any("FROM" in e for e in result.errors)

    def test_empty_sql(self, validator):
        result = validator.validate(EMPTY_SQL)
        assert not result.is_valid
        assert result.errors

    def test_comment_only_sql(self, validator):
        result = validator.validate(COMMENT_ONLY)
        assert not result.is_valid

    def test_select_star_produces_warning(self, validator):
        result = validator.validate("SELECT * FROM `mydb.users`")
        # May be valid but should warn
        assert any("SELECT *" in w or "star" in w.lower() for w in result.warnings)
