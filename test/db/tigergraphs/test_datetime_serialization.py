"""Tests for datetime serialization in TigerGraph connection.

This module tests that datetime objects are properly serialized when
upserting vertices and edges to TigerGraph.
"""

from datetime import date, datetime, time

import pytest

from graflo.db.util import json_serializer

# Alias for backward compatibility with test
_json_serializer = json_serializer


class TestDatetimeSerialization:
    """Test suite for datetime serialization in TigerGraph."""

    def test_json_serializer_datetime(self):
        """Test that datetime objects are serialized to ISO format."""
        dt = datetime(2023, 12, 25, 14, 30, 45)
        result = _json_serializer(dt)
        assert result == "2023-12-25T14:30:45"
        assert isinstance(result, str)

    def test_json_serializer_date(self):
        """Test that date objects are serialized to ISO format."""
        d = date(2023, 12, 25)
        result = _json_serializer(d)
        assert result == "2023-12-25"
        assert isinstance(result, str)

    def test_json_serializer_time(self):
        """Test that time objects are serialized to ISO format."""
        t = time(14, 30, 45)
        result = _json_serializer(t)
        assert result == "14:30:45"
        assert isinstance(result, str)

    def test_json_serializer_decimal(self):
        """Test that Decimal objects are converted to float."""
        from decimal import Decimal

        dec = Decimal("123.456")
        result = _json_serializer(dec)
        assert result == 123.456
        assert isinstance(result, float)

    def test_json_serializer_unsupported_type(self):
        """Test that unsupported types raise TypeError."""
        with pytest.raises(TypeError, match="not serializable"):
            _json_serializer(object())

    def test_json_dumps_with_datetime(self):
        """Test that json.dumps works with datetime objects using the serializer."""
        import json

        data = {
            "id": "test1",
            "name": "Test User",
            "created_at": datetime(2023, 12, 25, 14, 30, 45),
            "birth_date": date(2023, 12, 25),
            "login_time": time(14, 30, 45),
        }

        # Should not raise an error
        json_str = json.dumps(data, default=_json_serializer)
        assert isinstance(json_str, str)

        # Should be able to parse it back
        parsed = json.loads(json_str)
        assert parsed["id"] == "test1"
        assert parsed["name"] == "Test User"
        assert parsed["created_at"] == "2023-12-25T14:30:45"
        assert parsed["birth_date"] == "2023-12-25"
        assert parsed["login_time"] == "14:30:45"

    def test_json_dumps_without_serializer_raises_error(self):
        """Test that json.dumps without serializer raises TypeError for datetime."""
        import json

        data = {
            "id": "test1",
            "created_at": datetime(2023, 12, 25, 14, 30, 45),
        }

        # Should raise TypeError without the serializer
        with pytest.raises(TypeError):
            json.dumps(data)
