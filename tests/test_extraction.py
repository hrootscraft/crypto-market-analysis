"""Unit tests for CoinGecko extraction functions.

Tests use mocking to avoid hitting the actual API.
Run with: pytest tests/ -v
"""
import pytest
from unittest.mock import patch, Mock

from extract.coingecko import extract_coin_history, extract_exchange_volumes


class TestExtractCoinHistory:
    """Tests for extract_coin_history()."""

    MOCK_RESPONSE = {
        "prices": [
            [1704067200000, 42000.00],
            [1704153600000, 43500.50],
        ],
        "market_caps": [
            [1704067200000, 820000000000.0],
            [1704153600000, 850000000000.0],
        ],
        "total_volumes": [
            [1704067200000, 15000000000.0],
            [1704153600000, 18000000000.0],
        ],
    }

    @patch("extract.coingecko.requests.get")
    def test_returns_correct_row_count(self, mock_get):
        """Should return one row per price data point."""
        mock_resp = Mock()
        mock_resp.json.return_value = self.MOCK_RESPONSE
        mock_resp.raise_for_status = Mock()
        mock_get.return_value = mock_resp

        rows = extract_coin_history(
            "https://api.coingecko.com/api/v3", "bitcoin", days=2
        )

        assert len(rows) == 2

    @patch("extract.coingecko.requests.get")
    def test_row_structure(self, mock_get):
        """Each row should have expected keys and types."""
        mock_resp = Mock()
        mock_resp.json.return_value = self.MOCK_RESPONSE
        mock_resp.raise_for_status = Mock()
        mock_get.return_value = mock_resp

        rows = extract_coin_history(
            "https://api.coingecko.com/api/v3", "bitcoin", days=2
        )
        row = rows[0]

        assert row["coin_id"] == "bitcoin"
        assert row["price_date"] == "2024-01-01"
        assert row["price_usd"] == 42000.00
        assert row["volume_usd"] == 15000000000.0
        assert "market_cap_usd" in row

    @patch("extract.coingecko.requests.get")
    def test_handles_api_error(self, mock_get):
        """Should raise HTTPError after retries exhausted."""
        import requests

        mock_resp = Mock()
        mock_resp.raise_for_status.side_effect = requests.HTTPError(
            "429 Too Many Requests"
        )
        mock_get.return_value = mock_resp

        with pytest.raises(requests.HTTPError):
            extract_coin_history(
                "https://api.coingecko.com/api/v3",
                "bitcoin",
                max_retries=1,
                backoff_seconds=0.01,
            )


class TestExtractExchangeVolumes:
    """Tests for extract_exchange_volumes()."""

    MOCK_RESPONSE = [
        {
            "id": "binance",
            "name": "Binance",
            "trade_volume_24h_btc": 150000.5,
            "trust_score": 10,
        },
        {
            "id": "coinbase",
            "name": "Coinbase Exchange",
            "trade_volume_24h_btc": 45000.2,
            "trust_score": 10,
        },
    ]

    @patch("extract.coingecko.requests.get")
    def test_returns_exchange_data(self, mock_get):
        """Should parse exchange list into structured rows."""
        mock_resp = Mock()
        mock_resp.json.return_value = self.MOCK_RESPONSE
        mock_resp.raise_for_status = Mock()
        mock_get.return_value = mock_resp

        rows = extract_exchange_volumes("https://api.coingecko.com/api/v3")

        assert len(rows) == 2
        assert rows[0]["exchange_id"] == "binance"
        assert rows[1]["exchange_name"] == "Coinbase Exchange"
        assert rows[0]["volume_btc_24h"] == 150000.5
