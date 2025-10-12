import pytest
from core.utils import convert_standing, create_feature_vector

def test_convert_standing():
    """Проверка конвертации статистики команды"""
    standing_data = {
        'team': 'Test Team',
        'games_played': 10,
        'games_wins': 7,
        'games_draws': 1,
        'games_losses': 2,
        'goals_scored': 20,
        'goals_conceded': 8,
        'points': 22
    }
    
    result = convert_standing({'generaltablestrategy': standing_data}, 'Test Team')
    assert result['games_played'] == 10
    assert result['win_percentage'] == 0.7  # 7/10

def test_create_feature_vector():
    """Проверка создания вектора признаков"""
    home_data = {
        'games_played': 10,
        'win_percentage': 0.7,
        'goal_difference': 12
    }
    
    away_data = {
        'games_played': 10,
        'win_percentage': 0.3,
        'goal_difference': -5
    }
    
    feature_vector = create_feature_vector(home_data, away_data)
    
    assert 'home_advantage' in feature_vector
    assert 'goal_diff_ratio' in feature_vector
    assert feature_vector['win_rate_diff'] > 0  # домашняя команда сильнее
