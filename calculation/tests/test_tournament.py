import pytest
from standings import Tournament, Team, Match
from datetime import datetime

def test_singleton_tournament():
    """Проверка, что Tournament реализован как Singleton"""
    t1 = Tournament()
    t2 = Tournament()
    assert t1 is t2

def test_add_team():
    """Проверка добавления команд в турнир"""
    tournament = Tournament()
    tournament.add_team("Team A")
    assert "Team A" in tournament.teams
    assert isinstance(tournament.teams["Team A"], Team)

def test_get_team():
    """Проверка получения команды из турнира"""
    tournament = Tournament()
    tournament.add_team("Team A")
    team = tournament.get_team("Team A")
    assert team is not None
    assert team.name == "Team A"

def test_add_match():
    """Проверка добавления матча в турнир"""
    tournament = Tournament()
    tournament.add_team("Team A")
    tournament.add_team("Team B")
    
    match = Match(
        1, 1, 1, 1,
        datetime(2023, 1, 1),
        tournament.get_team("Team A"),
        tournament.get_team("Team B"),
        2, 1, "", 1, 1
    )
    match.play()
    
    team_a = tournament.get_team("Team A")
    team_b = tournament.get_team("Team B")
    
    assert team_a.games_played == 1
    assert team_b.games_played == 1
    assert team_a.points == 3
    assert team_b.points == 0
