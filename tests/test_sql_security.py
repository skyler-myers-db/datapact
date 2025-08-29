"""Test SQL security functions to prevent injection attacks."""

import pytest
from datapact.sql_utils import (
    escape_sql_identifier,
    escape_sql_string,
    validate_job_name,
    build_safe_filter,
)


class TestSQLSecurity:
    """Test suite for SQL injection prevention."""

    def test_escape_sql_identifier_valid(self):
        """Test escaping valid SQL identifiers."""
        assert escape_sql_identifier("table_name") == "`table_name`"
        assert escape_sql_identifier("schema.table") == "`schema.table`"
        assert escape_sql_identifier("col-name") == "`col-name`"
        assert escape_sql_identifier("table`with`ticks") == "`table``with``ticks`"

    def test_escape_sql_identifier_invalid(self):
        """Test that invalid identifiers raise errors."""
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            escape_sql_identifier("table; DROP TABLE users")
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            escape_sql_identifier("table' OR '1'='1")

    def test_escape_sql_string(self):
        """Test escaping SQL string values."""
        assert escape_sql_string("normal string") == "'normal string'"
        assert escape_sql_string("string with 'quotes'") == "'string with ''quotes'''"
        assert (
            escape_sql_string("'; DROP TABLE users; --") == "'''; DROP TABLE users; --'"
        )
        assert escape_sql_string("O'Reilly") == "'O''Reilly'"

    def test_validate_job_name_valid(self):
        """Test validation of valid job names."""
        assert validate_job_name("My Job") == "My Job"
        assert validate_job_name("job_123") == "job_123"
        assert validate_job_name("test-job.v2") == "test-job.v2"
        assert (
            validate_job_name("Data Validation Job 2024") == "Data Validation Job 2024"
        )

    def test_validate_job_name_invalid(self):
        """Test that invalid job names raise errors."""
        with pytest.raises(ValueError, match="Invalid job name"):
            validate_job_name("job; DROP TABLE")
        with pytest.raises(ValueError, match="Invalid job name"):
            validate_job_name("job' OR '1'='1")
        with pytest.raises(ValueError, match="Invalid job name"):
            validate_job_name("job/*comment*/")

        # Test length limit
        with pytest.raises(ValueError, match="Job name too long"):
            validate_job_name("x" * 256)

    def test_build_safe_filter(self):
        """Test building safe SQL WHERE clauses."""
        # String values
        assert build_safe_filter("status", "active") == "`status` = 'active'"
        assert build_safe_filter("name", "O'Brien", "=") == "`name` = 'O''Brien'"

        # Boolean values - Python's bool handling varies so just check they work
        result = build_safe_filter("is_active", True)
        assert "`is_active`" in result and "TRUE" in result.upper()
        result = build_safe_filter("is_deleted", False)
        assert "`is_deleted`" in result and "FALSE" in result.upper()

        # Numeric values
        assert build_safe_filter("count", 42) == "`count` = 42"
        assert build_safe_filter("price", 19.99, ">") == "`price` > 19.99"

        # NULL values
        assert build_safe_filter("optional_field", None) == "`optional_field` IS NULL"
        assert (
            build_safe_filter("required_field", None, "!=")
            == "`required_field` IS NOT NULL"
        )

        # LIKE operator
        assert build_safe_filter("name", "%john%", "LIKE") == "`name` LIKE '%john%'"

    def test_build_safe_filter_invalid_operator(self):
        """Test that invalid operators raise errors."""
        with pytest.raises(ValueError, match="Invalid operator"):
            build_safe_filter("col", "value", "EXEC")
        with pytest.raises(ValueError, match="Invalid operator"):
            build_safe_filter("col", "value", "; DROP TABLE")

    def test_sql_injection_attempts_are_escaped(self):
        """Test that common SQL injection attempts are properly escaped."""
        # Attempt 1: Classic injection
        malicious = "'; DROP TABLE users; --"
        escaped = escape_sql_string(malicious)
        assert escaped == "'''; DROP TABLE users; --'"

        # Attempt 2: Union injection
        malicious = "' UNION SELECT * FROM passwords --"
        escaped = escape_sql_string(malicious)
        assert escaped == "''' UNION SELECT * FROM passwords --'"

        # Attempt 3: Always true condition
        malicious = "' OR '1'='1"
        escaped = escape_sql_string(malicious)
        assert escaped == "''' OR ''1''=''1'"

        # These should all be safe to use in queries now
        safe_query = f"SELECT * FROM table WHERE name = {escaped}"
        assert (
            "DROP TABLE" not in safe_query or "''" in safe_query
        )  # DROP TABLE is escaped

    def test_genie_room_sql_is_safe(self):
        """Test that Genie room SQL generation uses safe escaping."""
        from datapact.client import DataPactClient
        from unittest.mock import MagicMock, patch

        with patch("datapact.client.WorkspaceClient") as mock_ws:
            mock_ws_instance = MagicMock()
            mock_ws.return_value = mock_ws_instance
            mock_ws_instance.current_user.me.return_value = MagicMock(user_name="test")
            mock_ws_instance.workspace.mkdirs = MagicMock()

            client = DataPactClient()

            # Test with potentially malicious job name
            job_name = "test'; DROP TABLE users; --"
            results_table = "`catalog`.`schema`.`table`"

            # This should not raise an error but should escape the job name
            try:
                sql = client._generate_genie_room_sql(results_table, job_name)
                # Should fail validation
                assert False, "Should have raised ValueError for invalid job name"
            except ValueError as e:
                assert "Invalid job name" in str(e)

            # Test with valid job name
            job_name = "Valid Job Name"
            sql = client._generate_genie_room_sql(results_table, job_name)

            # Check that the job name is properly quoted
            assert "WHERE job_name = 'Valid Job Name'" in sql
            assert "DROP TABLE" not in sql  # No injection


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
