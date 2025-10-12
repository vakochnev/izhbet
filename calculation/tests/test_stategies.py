import pytest
from standings import (
    GeneralTableStrategy, HomeGamesTableStrategy, AwayGamesTableStrategy,
    StrongOpponentsTableStrategy, Team
)

def create_test_teams():
    """Создание тестовых команд с разной статистикой"""
    teams = []
    for i in range(5):
        team = Team(f"Team {i}")
        team.points = (5 - i) * 3  # Разные очки для каждой команды
        team.matches = []  # Инициализируем пустым списком
        teams.append(team)
    
    # Добавляем некоторые матчи для тестирования
    for i in range(4):
        home_team = teams[i]
        away_team = teams[i+1]
        
        # Домашние матчи
        home_match = (2, 1, 'win', True, away_team, i, None)
        home_team.matches.append(home_match)
        away_match = (1, 2, 'loss', False, home_team, i, None)
        away_team.matches.append(away_match)
        
        # Гостевые матчи
        away_match = (1, 3, 'loss', False, home_team, i+5, None)
        home_team.matches.append(away_match)
        home_match = (3, 1, 'win', True, away_team, i+5, None)
        away_team.matches.append(home_match)
    
    return teams

def test_general_table_strategy():
    """Проверка общей стратегии таблицы"""
    strategy = GeneralTableStrategy()
    teams = create_test_teams()
    
    strategy.filter_matches(teams)
    
    for team in teams:
        assert len(team.filtered_matches) == len(team.matches)

def test_home_games_strategy():
    """Проверка стратегии домашних игр"""
    strategy = HomeGamesTableStrategy()
    teams = create_test_teams()
    
    strategy.filter_matches(teams)
    
    for team in teams:
        assert all(match[3] for match in team.filtered_matches)
        assert len(team.filtered_matches) == len(team.matches) // 2

def test_away_games_strategy():
    """Проверка стратегии гостевых игр"""
    strategy = AwayGamesTableStrategy()
    teams = create_test_teams()
    
    strategy.filter_matches(teams)
    
    for team in teams:
        assert all(not match[3] for match in team.filtered_matches)
        assert len(team.filtered_matches) == len(team.matches) // 2

def test_strong_opponents_strategy():
    """Проверка стратегии сильных соперников"""
    strategy = StrongOpponentsTableStrategy()
    teams = create_test_teams()
    
    strategy.filter_matches(teams)
    
    # Проверяем, что фильтруются только матчи с сильными соперниками
    strong_teams = strategy.get_strong_teams(teams)
    for team in teams:
        for match in team.filtered_matches:
            assert match[4] in strong_teams
