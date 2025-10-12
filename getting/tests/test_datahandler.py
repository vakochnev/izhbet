# tests/test_datahandler.py
import pytest
from getting.datahandler import (
    SportHandler,
    CountryHandler,
    TournamentHandler,
    ChampionshipHandler,
    MatchHandler,
    TeamHandler,
    GoalHandler,
    PeriodHandler,
    CoefHandler,
    TableHandler,
    DataHandlerFactory,
)


def test_sport_handler_preparing_data(sample_api_data):
    handler = SportHandler()
    result = handler.preparing_data(sample_api_data)
    assert result["id"] == "sr:sport:1"


def test_country_handler_preparing_data(sample_api_data, monkeypatch):
    def mock_is_country_top(country_id):
        return country_id == "sr:country:1"
    monkeypatch.setattr("db.queries.country.is_country_top", mock_is_country_top)
    handler = CountryHandler()
    result = handler.preparing_data(sample_api_data)
    assert len(result) == 1
    assert result[0]["id"] == "sr:country:1"


def test_tournament_handler_preparing_data(sample_api_data):
    handler = TournamentHandler()
    result = handler.preparing_data(sample_api_data)
    assert len(result) == 2


def test_championship_handler_preparing_data(sample_api_data, monkeypatch):
    def mock_is_chmpionship_top(tournament_id):
        return tournament_id == "sr:tournament:1"
    monkeypatch.setattr("db.queries.championship.is_chmpionship_top", mock_is_chmpionship_top)
    handler = ChampionshipHandler()
    result = handler.preparing_data(sample_api_data)
    assert len(result) == 1
    assert result[0]["id"] == "sr:tournament:1"


def test_match_handler_preparing_data(sample_api_data):
    handler = MatchHandler()
    result = handler.preparing_data(sample_api_data)
    assert len(result) == 1


def test_team_handler_preparing_data(sample_api_data):
    handler = TeamHandler()
    result = handler.preparing_data(sample_api_data)
    assert len(result) == 0  # Тестовые данные не содержат 'teams'


def test_goal_handler_preparing_data(sample_api_data):
    handler = GoalHandler()
    result = handler.preparing_data(sample_api_data)
    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0][0]["match_id"] == "sr:match:1"


def test_period_handler_preparing_data(sample_api_data):
    handler = PeriodHandler()
    result = handler.preparing_data(sample_api_data)
    assert len(result) == 1
    assert result[0][0]["match_id"] == "sr:match:1"


def test_coef_handler_preparing_data(sample_api_data):
    handler = CoefHandler()
    result = handler.preparing_data(sample_api_data, coefs_id=100)
    assert result[0][0]["id"] == 100


def test_table_handler_preparing_data(sample_api_data):
    handler = TableHandler()
    result = handler.preparing_data(sample_api_data)
    assert isinstance(result, tuple)
    assert result[0] is None  # Нет ключа 'id' в данных по умолчанию


def test_datahandler_factory_create_handler():
    handler = DataHandlerFactory.create_handler("sport")
    assert isinstance(handler, SportHandler)

    with pytest.raises(ValueError):
        DataHandlerFactory.create_handler("invalid_type")
