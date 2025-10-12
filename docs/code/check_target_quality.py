#!/usr/bin/env python3
import logging
import pandas as pd
from typing import Dict, Any

from config import Session_pool
from db.models import Feature, Match
from core.constants import TARGET_FIELDS

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def check_target_distribution() -> None:
    """Проверяет распределение таргетов."""
    print("\n🎯 ПРОВЕРКА КАЧЕСТВА ТАРГЕТОВ")
    print("=" * 50)
    
    with Session_pool() as db:
        # Загружаем данные фичей
        features = db.query(Feature).filter(Feature.prefix == 'home').limit(10000).all()
        
        if not features:
            print("❌ Нет данных фичей")
            return
        
        df = pd.DataFrame([f.as_dict() for f in features])
        
        # Проверяем каждый таргет
        target_checks = {
            'win_draw_loss': ['target_win_draw_loss_home_win', 'target_win_draw_loss_draw', 'target_win_draw_loss_away_win'],
            'oz': ['target_oz_both_score', 'target_oz_not_both_score'],
            'goal_home': ['target_goal_home_yes', 'target_goal_home_no'],
            'goal_away': ['target_goal_away_yes', 'target_goal_away_no'],
            'total': ['target_total_over', 'target_total_under'],
            'total_home': ['target_total_home_over', 'target_total_home_under'],
            'total_away': ['target_total_away_over', 'target_total_away_under'],
            'total_amount': ['target_total_amount'],
            'total_home_amount': ['target_total_home_amount'],
            'total_away_amount': ['target_total_away_amount']
        }
        
        for target_name, columns in target_checks.items():
            print(f"\n📊 {target_name.upper()}:")
            print("-" * 30)
            
            for col in columns:
                if col in df.columns:
                    values = df[col].value_counts(dropna=False)
                    null_count = df[col].isnull().sum()
                    
                    print(f"  {col}:")
                    for val, count in values.items():
                        pct = count / len(df) * 100
                        print(f"    {val}: {count:,} ({pct:.1f}%)")
                    
                    if null_count > 0:
                        print(f"    NULL: {null_count:,} ({null_count/len(df)*100:.1f}%)")
                else:
                    print(f"  {col}: НЕТ В ДАННЫХ")


def check_target_consistency() -> None:
    """Проверяет консистентность таргетов."""
    print("\n🔍 ПРОВЕРКА КОНСИСТЕНТНОСТИ ТАРГЕТОВ")
    print("=" * 50)
    
    with Session_pool() as db:
        # Загружаем данные фичей
        features = db.query(Feature).filter(Feature.prefix == 'home').limit(10000).all()
        
        if not features:
            print("❌ Нет данных фичей")
            return
        
        df = pd.DataFrame([f.as_dict() for f in features])
        
        # Проверяем one-hot кодировку
        print("\n📋 One-hot кодировка:")
        
        # Win/Draw/Loss
        wdl_cols = ['target_win_draw_loss_home_win', 'target_win_draw_loss_draw', 'target_win_draw_loss_away_win']
        if all(col in df.columns for col in wdl_cols):
            wdl_sum = df[wdl_cols].sum(axis=1)
            invalid_wdl = (wdl_sum != 1).sum()
            print(f"  Win/Draw/Loss: {invalid_wdl:,} некорректных записей (сумма != 1)")
        
        # OZ
        oz_cols = ['target_oz_both_score', 'target_oz_not_both_score']
        if all(col in df.columns for col in oz_cols):
            oz_sum = df[oz_cols].sum(axis=1)
            invalid_oz = (oz_sum != 1).sum()
            print(f"  OZ: {invalid_oz:,} некорректных записей (сумма != 1)")
        
        # Goal Home
        gh_cols = ['target_goal_home_yes', 'target_goal_home_no']
        if all(col in df.columns for col in gh_cols):
            gh_sum = df[gh_cols].sum(axis=1)
            invalid_gh = (gh_sum != 1).sum()
            print(f"  Goal Home: {invalid_gh:,} некорректных записей (сумма != 1)")
        
        # Goal Away
        ga_cols = ['target_goal_away_yes', 'target_goal_away_no']
        if all(col in df.columns for col in ga_cols):
            ga_sum = df[ga_cols].sum(axis=1)
            invalid_ga = (ga_sum != 1).sum()
            print(f"  Goal Away: {invalid_ga:,} некорректных записей (сумма != 1)")
        
        # Total
        total_cols = ['target_total_over', 'target_total_under']
        if all(col in df.columns for col in total_cols):
            total_sum = df[total_cols].sum(axis=1)
            invalid_total = (total_sum != 1).sum()
            print(f"  Total: {invalid_total:,} некорректных записей (сумма != 1)")
        
        # Total Home
        th_cols = ['target_total_home_over', 'target_total_home_under']
        if all(col in df.columns for col in th_cols):
            th_sum = df[th_cols].sum(axis=1)
            invalid_th = (th_sum != 1).sum()
            print(f"  Total Home: {invalid_th:,} некорректных записей (сумма != 1)")
        
        # Total Away
        ta_cols = ['target_total_away_over', 'target_total_away_under']
        if all(col in df.columns for col in ta_cols):
            ta_sum = df[ta_cols].sum(axis=1)
            invalid_ta = (ta_sum != 1).sum()
            print(f"  Total Away: {invalid_ta:,} некорректных записей (сумма != 1)")


def check_target_vs_actual() -> None:
    """Проверяет соответствие таргетов фактическим результатам матчей."""
    print("\n🎲 ПРОВЕРКА ТАРГЕТОВ VS ФАКТИЧЕСКИЕ РЕЗУЛЬТАТЫ")
    print("=" * 50)
    
    with Session_pool() as db:
        # Загружаем данные фичей с матчами
        query = db.query(Feature, Match).join(Match, Feature.match_id == Match.id).filter(
            Feature.prefix == 'home',
            Match.numOfHeadsHome.isnot(None),
            Match.numOfHeadsAway.isnot(None)
        ).limit(1000)
        
        results = query.all()
        
        if not results:
            print("❌ Нет данных матчей с результатами")
            return
        
        print(f"📊 Проверено матчей: {len(results)}")
        
        # Проверяем несколько примеров
        print("\n🔍 Примеры проверки:")
        print("-" * 50)
        
        for i, (feature, match) in enumerate(results[:10]):
            home_goals = match.numOfHeadsHome
            away_goals = match.numOfHeadsAway
            total_goals = home_goals + away_goals
            
            print(f"\nМатч {match.id}: {home_goals}:{away_goals}")
            
            # Win/Draw/Loss
            if feature.target_win_draw_loss_home_win == 1:
                predicted = "П1"
            elif feature.target_win_draw_loss_draw == 1:
                predicted = "Н"
            elif feature.target_win_draw_loss_away_win == 1:
                predicted = "П2"
            else:
                predicted = "НЕ ОПРЕДЕЛЕНО"
            
            actual = "П1" if home_goals > away_goals else ("Н" if home_goals == away_goals else "П2")
            print(f"  WDL: прогноз={predicted}, факт={actual}, {'✅' if predicted == actual else '❌'}")
            
            # OZ
            oz_predicted = "ДА" if feature.target_oz_both_score == 1 else "НЕТ"
            oz_actual = "ДА" if home_goals > 0 and away_goals > 0 else "НЕТ"
            print(f"  OZ: прогноз={oz_predicted}, факт={oz_actual}, {'✅' if oz_predicted == oz_actual else '❌'}")
            
            # Total
            total_predicted = "ТБ" if feature.target_total_over == 1 else "ТМ"
            total_actual = "ТБ" if total_goals > 2.5 else "ТМ"  # Предполагаем порог 2.5
            print(f"  Total: прогноз={total_predicted}, факт={total_actual}, {'✅' if total_predicted == total_actual else '❌'}")


def main():
    check_target_distribution()
    check_target_consistency()
    check_target_vs_actual()


if __name__ == '__main__':
    main()
