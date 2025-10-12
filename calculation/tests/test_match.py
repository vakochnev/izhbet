import pytest
from standings import Team, Match
from datetime import datetime

def test_match_play():
    """Проверка корректного обновления статистики после игры"""
    home_team = Team("Team A")
    away_team = Team("Team B")
    
    match = Match(
        1, 1, 1, 1,
        datetime(2023, 1, 1),
        home_team,
        away_team,
        2, 1, "", 1, 1
    )
    match.play()
    
    assert home_team.games_played == 1
    assert away_team.games_played == 1
    assert home_team.points == 3
    assert away_team.points == 0
    assert home_team.goals_scored == 2
    assert away_team.goals_scored == 1
