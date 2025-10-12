import pytest
from standings import Team, Match, Tournament
from datetime import datetime

@pytest.fixture
def sample_teams():
    """Создает две тестовые команды"""
    team_a = Team("Team A")
    team_b = Team("Team B")
    return team_a, team_b

@pytest.fixture
def sample_match(sample_teams):
    """Создает тестовый матч между двумя командами"""
    team_a, team_b = sample_teams
    match = Match(
        id_match=1,
        sport_id=1,
        country_id=1,
        tournament_id=1,
        game_data=datetime(2023, 1, 1),
        home_team=team_a,
        away_team=team_b,
        home_goals=2,
        away_goals=1,
        overtime="",
        season_id=1,
        stages_id=1
    )
    return match

@pytest.fixture
def tournament_with_teams():
    """Создает турнир с несколькими командами"""
    tournament = Tournament()
    teams = [Team(f"Team {i}") for i in range(5)]
    for team in teams:
        tournament.add_team(team.name)
    return tournament, teams
