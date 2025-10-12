import pytest
from standings import Team, Match
from datetime import datetime

def test_team_initialization():
    team = Team("Test Team")
    assert team.name == "Test Team"
    assert team.games_played == 0

def test_match_updates_stats(sample_match):
    sample_match.play()
    assert sample_match.home_team.goals_scored == 2
    assert sample_match.away_team.goals_scored == 1
    assert sample_match.home_team.points == 3
    assert sample_match.away_team.points == 0

def test_update_stats_win():
    team = Team("Team A")
    opponent = Team("Team B")
    team.update_stats(3, 1, 'win', True, opponent, '', 1, datetime.now())
    assert team.games_wins == 1
    assert team.points == 3
