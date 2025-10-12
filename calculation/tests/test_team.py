import pytest
from standings import Team
from datetime import datetime

def test_team_initialization():
    """Проверка инициализации команды"""
    team = Team("Team A")
    assert team.name == "Team A"
    assert team.games_played == 0
    assert team.goals_scored == 0

def test_update_stats_win():
    """Проверка обновления статистики при победе"""
    team = Team("Team A")
    opponent = Team("Team B")
    team.update_stats(2, 1, 'win', True, opponent, '', 1, datetime.now())
    
    assert team.games_played == 1
    assert team.games_wins == 1
    assert team.goals_scored == 2
    assert team.goals_conceded == 1
    assert team.points == 3

def test_update_stats_draw():
    """Проверка обновления статистики при ничье"""
    team = Team("Team A")
    opponent = Team("Team B")
    team.update_stats(1, 1, 'draw', False, opponent, '', 1, datetime.now())
    
    assert team.games_played == 1
    assert team.games_draws == 1
    assert team.points == 1

def test_update_stats_loss():
    """Проверка обновления статистики при проигрыше"""
    team = Team("Team A")
    opponent = Team("Team B")
    team.update_stats(0, 2, 'loss', True, opponent, '', 1, datetime.now())
    
    assert team.games_played == 1
    assert team.games_losses == 1
    assert team.lossing_dry == 1  # не забила
    assert team.points == 0

def test_goal_properties():
    """Проверка свойств, связанных с голами"""
    team = Team("Team A")
    opponent = Team("Team B")
    
    team.update_stats(2, 1, 'win', True, opponent, '', 1, datetime.now())
    team.update_stats(1, 2, 'loss', False, opponent, '', 1, datetime.now())
    
    assert team.get_goal_difference == (3 - 3)  # 2+1 и 1+2
    assert team.get_goal_amount == 6
    assert team.get_goal_ratio == 1.0  # одинаковое количество голов
