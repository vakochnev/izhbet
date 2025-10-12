# tests/conftest.py
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture
def mock_requests_get_success():
    """Мок успешного ответа requests.get."""
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"sports": [{"id": "sr:sport:1", "name": "Basketball"}]}
        mock_get.return_value = mock_response
        yield mock_get


@pytest.fixture
def mock_db_session():
    """Мок сессии базы данных."""
    with patch("config.db_session") as mock_session:
        mock_session.commit_session = MagicMock()
        yield mock_session


@pytest.fixture
def mock_sport_handler():
    from getting.datahandler import SportHandler
    return SportHandler()


@pytest.fixture
def mock_country_handler():
    from getting.datahandler import CountryHandler
    return CountryHandler()


@pytest.fixture
def sample_api_data():
    return {
        "sports": [
            {"id": "sr:sport:1", "name": "Basketball"},
            {"id": "sr:sport:2", "name": "Tennis"},
        ],
        "categories": [
            {"id": "sr:country:1", "name": "USA"},
            {"id": "sr:country:2", "name": "Russia"},
        ],
        "tournaments": [
            {"id": "sr:tournament:1", "name": "NBA"},
            {"id": "sr:tournament:2", "name": "EuroLeague"},
        ],
        "matches": [
            {
                "id": "sr:match:1",
                "homeId": "sr:team:1",
                "awayId": "sr:team:2",
                "goals": [{"team": "home", "player": "John Doe"}],
                "periods": [{"type": "1H", "score": "10-5"}],
            }
        ],
        "main": [
            {"coefs": [{"value": "2.5"}]},
        ]
    }
