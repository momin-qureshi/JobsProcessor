import pytest
import json
from unittest.mock import MagicMock, patch
from process_bucket import process_file


# Fixtures
@pytest.fixture
def mock_s3():
    """
    Fixture for mocking the S3 object.
    """
    return MagicMock()


@pytest.fixture
def mock_cache():
    """
    Fixture for mocking the cache object.
    """
    return MagicMock()


# Test Cases
def test_process_file_success(mock_s3, mock_cache):
    """
    Test case for successful file processing.
    """
    key = "test_file.json"
    raw_data = (
        '{"company": "CompanyA", "title": "Software Engineer"}'
        '\n{"company": "CompanyB", "title": "Data Scientist"}'
    )
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=raw_data.encode("utf-8")))
    }
    mock_cache.get.side_effect = ["Senior", "Junior"]

    with patch("process_bucket.s3", mock_s3), patch(
        "process_bucket.cache", mock_cache
    ), patch("process_bucket.infer_seniorities", return_value=["Senior", "Junior"]):
        result = process_file(key)
        assert result is True
        mock_s3.put_object.assert_called_once()


def test_process_file_invalid_json(mock_s3, mock_cache):
    """
    Test case for handling invalid JSON in the file.
    """
    key = "test_file.json"
    raw_data = (
        '{"company": "CompanyA", "title": "Software Engineer"'
        '\n{"company": "CompanyB", "title": "Data Scientist"}'
    )  # Invalid JSON
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=raw_data.encode("utf-8")))
    }

    with patch("process_bucket.s3", mock_s3), patch("process_bucket.cache", mock_cache):
        with pytest.raises(json.JSONDecodeError):
            process_file(key)


def test_process_file_infer_seniorities_fail(mock_s3, mock_cache):
    """
    Test case for failing to infer seniorities.
    """
    key = "test_file.json"
    raw_data = (
        '{"company": "CompanyA", "title": "Software Engineer"}'
        '\n{"company": "CompanyB", "title": "Data Scientist"}'
    )

    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=MagicMock(return_value=raw_data.encode("utf-8")))
    }

    with patch("process_bucket.s3", mock_s3), patch(
        "process_bucket.cache", mock_cache
    ), patch("process_bucket.infer_seniorities", return_value=[]):
        result = process_file(key)
        assert result is False
        mock_s3.put_object.assert_not_called()
