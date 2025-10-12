#!/usr/bin/env python3
import logging
import os
from datetime import datetime
import numpy as np
import pandas as pd
from typing import Dict, Tuple

from sklearn.feature_selection import (
    f_regression, mutual_info_regression,
    f_classif, mutual_info_classif
)

from config import Session_pool
from db.models import Feature
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


META = {'id', 'match_id', 'prefix', 'created_at', 'updated_at'}


def load_unified(limit_matches: int = 8000) -> pd.DataFrame:
    with Session_pool() as db:
        match_ids = (
            db.query(Feature.match_id)
            .group_by(Feature.match_id)
            .limit(limit_matches)
            .all()
        )
        match_ids = [r[0] for r in match_ids]
        if not match_ids:
            return pd.DataFrame()

        rows = (
            db.query(Feature)
            .filter(Feature.match_id.in_(match_ids))
            .all()
        )

    recs: Dict[int, Dict[str, float]] = {}
    for r in rows:
        d = r.as_dict()
        mid = d['match_id']
        pref = d['prefix']
        rec = recs.setdefault(mid, {'match_id': mid})
        for k, v in d.items():
            if k in META or k in DROP_FIELD_EMBEDDING or k.startswith('_'):
                continue
            # таргеты
            if pref == 'home':
                if k == 'target_total_amount':
                    rec['target_total_amount'] = v
                if k == 'target_total_home_amount':
                    rec['target_total_home_amount'] = v
                if k == 'target_total_away_amount':
                    rec['target_total_away_amount'] = v
                if k == 'target_win_draw_loss_home_win' and v is not None and int(v) == 1:
                    rec['target_wdl'] = 0
                if k == 'target_win_draw_loss_draw' and v is not None and int(v) == 1:
                    rec['target_wdl'] = 1
                if k == 'target_win_draw_loss_away_win' and v is not None and int(v) == 1:
                    rec['target_wdl'] = 2
                if k == 'target_oz_both_score' and v is not None and int(v) == 1:
                    rec['target_oz'] = 1
                if k == 'target_oz_not_both_score' and v is not None and int(v) == 1:
                    rec['target_oz'] = 0
            if k in TARGET_FIELDS:
                continue
            rec[f'{pref}_{k}'] = v

    df = pd.DataFrame.from_dict(recs, orient='index')
    # Приводим типы
    for c in df.columns:
        if c not in ('match_id', 'target_wdl', 'target_oz', 'target_total_amount', 'target_total_home_amount', 'target_total_away_amount'):
            df[c] = pd.to_numeric(df[c], errors='coerce')
    return df


def _export_scores(df_scores: pd.DataFrame, target_name: str) -> str:
    out_dir = os.path.join('results', 'quality', 'feature_importance')
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = os.path.join(out_dir, f'{target_name}_{ts}.csv')
    df_scores.to_csv(out_path, index=False)
    return out_path


def rank_regression(X: pd.DataFrame, y: pd.Series, title: str, target_key: str) -> None:
    f_vals, _ = f_regression(X, y)
    f_vals = np.nan_to_num(f_vals)
    mi_vals = mutual_info_regression(X, y, random_state=42)
    df_scores = pd.DataFrame({'feature': X.columns, 'F_value': f_vals, 'MI': mi_vals}).sort_values('MI', ascending=False)
    print(f"\n📊 {title}")
    print("-" * 70)
    print(f"Объектов: {len(y):,}, Фич: {X.shape[1]:,}")
    out_path = _export_scores(df_scores, target_key)
    print(f"CSV: {out_path}")
    for _, r in df_scores.head(30).iterrows():
        print(f"  {r['feature']}: MI={r['MI']:.5f}, F={r['F_value']:.2f}")


def rank_classif(X: pd.DataFrame, y: pd.Series, title: str, target_key: str) -> None:
    f_vals, _ = f_classif(X, y)
    f_vals = np.nan_to_num(f_vals)
    mi_vals = mutual_info_classif(X, y, random_state=42)
    df_scores = pd.DataFrame({'feature': X.columns, 'F_value': f_vals, 'MI': mi_vals}).sort_values('MI', ascending=False)
    print(f"\n📊 {title}")
    print("-" * 70)
    print(f"Объектов: {len(y):,}, Фич: {X.shape[1]:,}")
    out_path = _export_scores(df_scores, target_key)
    print(f"CSV: {out_path}")
    for _, r in df_scores.head(30).iterrows():
        print(f"  {r['feature']}: MI={r['MI']:.5f}, F={r['F_value']:.2f}")


def main() -> None:
    print("\n🧠 Важность признаков по всем целям")
    print("=" * 72)
    df = load_unified()
    if df.empty:
        print('Нет данных')
        return

    # общий признакный space
    base_cols = [c for c in df.columns if c not in ('match_id', 'target_wdl', 'target_oz', 'target_total_amount', 'target_total_home_amount', 'target_total_away_amount')]
    X_all = df[base_cols].fillna(0.0).astype(float)

    # win_draw_loss
    df_wdl = df[df['target_wdl'].isin([0,1,2])]
    if not df_wdl.empty:
        rank_classif(X_all.loc[df_wdl.index], df_wdl['target_wdl'].astype(int), 'win_draw_loss (f_classif, MI)', 'win_draw_loss')

    # oz
    df_oz = df[df['target_oz'].isin([0,1])]
    if not df_oz.empty:
        rank_classif(X_all.loc[df_oz.index], df_oz['target_oz'].astype(int), 'oz (f_classif, MI)', 'oz')

    # total_amount
    if 'target_total_amount' in df.columns:
        y = pd.to_numeric(df['target_total_amount'], errors='coerce').fillna(0.0).astype(float)
        rank_regression(X_all, y, 'total_amount (F, MI)', 'total_amount')

    # total_home_amount
    if 'target_total_home_amount' in df.columns:
        y = pd.to_numeric(df['target_total_home_amount'], errors='coerce').fillna(0.0).astype(float)
        rank_regression(X_all, y, 'total_home_amount (F, MI)', 'total_home_amount')

    # total_away_amount
    if 'target_total_away_amount' in df.columns:
        y = pd.to_numeric(df['target_total_away_amount'], errors='coerce').fillna(0.0).astype(float)
        rank_regression(X_all, y, 'total_away_amount (F, MI)', 'total_away_amount')


if __name__ == '__main__':
    main()


