import pytest
import numpy as np
from standings import (
    DIFRatingStrategy, VORatingStrategy, ELORatingStrategy,
    PotemkinRatingStrategy, PowerRatingStrategy, Team, Tournament
)

def create_test_teams():
    """Создание тестовых команд для проверки рейтингов"""
    teams = []
    for i in range(5):
        team = Team(f"Team {i}")
        team.matches = []  # Инициализируем пустым списком
        teams.append(team)
    return teams

def test_dif_rating_strategy():
    """Проверка стратегии DIF-рейтинга"""
    strategy = DIFRatingStrategy()
    teams = create_test_teams()
    
    # Ручное задание данных для тестирования
    for team in teams:
        team.filtered_matches = []
    
    # Добавляем фиктивные матчи
    teams[0].filtered_matches.append((2, 1, 'loss', False, teams[1], 1, None))
    teams[1].filtered_matches.append((3, 0, 'loss', True, teams[0], 1, None))
    
    strategy.calculate_ratings(teams)
    
    # Проверяем, что рейтинги установлены
    for team in teams:
        assert hasattr(team, 'dif_rating')
        assert isinstance(team.dif_rating, (int, float))

def test_vo_rating_strategy():
    """Проверка стратегии VO-рейтинга"""
    strategy = VORatingStrategy()
    teams = create_test_teams()
    
    # Назначаем очки для тестирования
    for i, team in enumerate(teams):
        team.filtered_matches = []
        team.points = (len(teams) - i) * 3  # Разные очки для разных команд
    
    strategy.calculate_ratings(teams)
    
    # Проверяем, что рейтинги установлены
    for team in teams:
        assert hasattr(team, 'vo_rating')
        assert isinstance(team.vo_rating, (int, float))

def test_elo_rating_strategy():
    """Проверка стратегии ЭЛО-рейтинга"""
    strategy = ELORatingStrategy()
    teams = create_test_teams()
    
    # Назначаем фильтрованные матчи
    for team in teams:
        team.filtered_matches = []
    
    # Создаем цепочку матчей между командами
    teams[0].filtered_matches.append((2, 1, 'win', True, teams[1], 1, None))
    teams[1].filtered_matches.append((1, 2, 'loss', False, teams[0], 1, None))
    teams[0].filtered_matches.append((1, 2, 'loss', False, teams[2], 1, None))
    teams[2].filtered_matches.append((2, 1, 'win', True, teams[0], 1, None))
    
    strategy.calculate_ratings(teams)
    
    # Проверяем, что рейтинги обновлены
    for team in teams:
        assert team.elo_rating != 1500  # Значение по умолчанию

def test_potemkin_rating_strategy():
    """Проверка стратегии Потёмкин-рейтинга"""
    strategy = PotemkinRatingStrategy()
    teams = create_test_teams()
    
    # Назначаем фильтрованные матчи
    for team in teams:
        team.filtered_matches = []
    
    # Создаем цепочку матчей между командами
    teams[0].filtered_matches.append((2, 1, 'win', True, teams[1], 1, None))
    teams[1].filtered_matches.append((1, 2, 'loss', False, teams[0], 1, None))
    
    strategy.calculate_ratings(teams)
    
    # Проверяем, что рейтинги обновлены
    for team in teams:
        assert team.potemkin_rating != 100  # Значение по умолчанию

def test_power_rating_strategy():
    """Проверка стратегии силового рейтинга"""
    strategy = PowerRatingStrategy()
    teams = create_test_teams()
    
    # Назначаем фильтрованные матчи
    for team in teams:
        team.filtered_matches = []
    
    # Создаем цепочку матчей между командами
    teams[0].filtered_matches.append((2, 1, 'win', True, teams[1], 1, None))
    teams[1].filtered_matches.append((1, 2, 'loss', False, teams[0], 1, None))
    
    strategy.calculate_ratings(teams)
    
    # Проверяем, что рейтинги обновлены
    for team in teams:
        assert team.power_rating != 0  # Значение по умолчанию
