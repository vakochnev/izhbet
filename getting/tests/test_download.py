# tests/test_download.py
import pytest
from getting.download import SportDataProcessing, MatchDataProcessing
from unittest.mock import MagicMock


def test_sportdataprocessing_process(mock_requests_get_success, mock_db_session):
    url = "http://mock.url/sports"
    processor = SportDataProcessing(url)
    processor.process()

    assert processor.sports["id"] == "sr:sport:1"
    assert len(processor.countrys) == 0  # Нет реализации is_country_top в тесте
    assert len(processor.championships) == 0


def test_matchdataprocessing_process(mock_requests_get_success, mock_db_session):
    class MockTournament:
        def __init__(self, id):
            self.id = id

    tournament = MockTournament(id="sr:tournament:1")
    processor = MatchDataProcessing(tournament)
    processor.matchs_handler.fetch_data = MagicMock(return_value={
        "matches": [{
            "id": "sr:match:1",
            "homeId": "sr:team:1",
            "awayId": "sr:team:2",
            "goals": [{"team": "home", "player": "John Doe"}],
            "result": []
        }]
    })

    processor.process()

    assert len(processor.teams) == 0
    assert len(processor.goals) == 1
    assert len(processor.periods) == 0
