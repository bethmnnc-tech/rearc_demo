"""
Unit tests for the pure-Python parts of ingestion/population_ingest.py.
Network calls are mocked -- these test our validation logic, not DataUSA's API.

Run with: pytest tests/ -v
"""

from unittest.mock import patch, MagicMock

import population_ingest as p


def _mock_response(json_payload, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_payload
    resp.raise_for_status = MagicMock()
    return resp


class TestFetchPopulationJson:
    def test_happy_path_returns_payload(self):
        payload = {"data": [{"Year": "2013", "Nation": "United States", "Population": 316128839}]}
        with patch.object(p.requests, "get", return_value=_mock_response(payload)):
            result = p.fetch_population_json("http://fake", {})
        assert result == payload

    def test_missing_data_key_raises(self):
        payload = {"unexpected": []}
        with patch.object(p.requests, "get", return_value=_mock_response(payload)):
            try:
                p.fetch_population_json("http://fake", {})
                assert False, "expected RuntimeError"
            except RuntimeError as e:
                assert "data" in str(e)

    def test_empty_data_array_raises(self):
        payload = {"data": []}
        with patch.object(p.requests, "get", return_value=_mock_response(payload)):
            try:
                p.fetch_population_json("http://fake", {})
                assert False, "expected RuntimeError"
            except RuntimeError:
                pass

    def test_data_not_a_list_raises(self):
        payload = {"data": {"Year": "2013"}}
        with patch.object(p.requests, "get", return_value=_mock_response(payload)):
            try:
                p.fetch_population_json("http://fake", {})
                assert False, "expected RuntimeError"
            except RuntimeError:
                pass

    def test_retries_on_500_then_succeeds(self):
        payload = {"data": [{"Year": "2013", "Population": 1}]}
        responses = [_mock_response({}, status_code=500), _mock_response(payload)]
        with patch.object(p.requests, "get", side_effect=responses), patch("time.sleep"):
            result = p.fetch_population_json("http://fake", {}, max_retries=3)
        assert result == payload
