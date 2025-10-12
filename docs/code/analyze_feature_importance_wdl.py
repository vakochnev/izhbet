#!/usr/bin/env python3
import logging
import numpy as np
import pandas as pd
from typing import Tuple

from sklearn.feature_selection import f_classif, mutual_info_classif

from config import Session_pool
from db.models import Feature
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


META = {'id', 'match_id', 'prefix', 'created_at', 'updated_at'}


def load_dataset(limit_matches: int = 5000) -> Tuple[pd.DataFrame, pd.Series]:
    with Session_pool() as db:
        match_ids = (
            db.query(Feature.match_id)
            .group_by(Feature.match_id)
            .limit(limit_matches)
            .all()
        )
        match_ids = [r[0] for r in match_ids]
        if not match_ids:
            return pd.DataFrame(), pd.Series(dtype=int)

        rows = (
            db.query(Feature)
            .filter(Feature.match_id.in_(match_ids))
            .all()
        )

    records = {}
    for r in rows:
        d = r.as_dict()
        mid = d['match_id']
        pref = d['prefix']
        rec = records.setdefault(mid, {'match_id': mid})
        for k, v in d.items():
            if k in META or k in DROP_FIELD_EMBEDDING or k.startswith('_'):
                continue
            # Собираем one-hot win/draw/loss как таргет
            if pref == 'home':
                if k == 'target_win_draw_loss_home_win' and v is not None:
                    rec['target_wdl'] = 0 if int(v) == 1 else rec.get('target_wdl')
                if k == 'target_win_draw_loss_draw' and v is not None:
                    rec['target_wdl'] = 1 if int(v) == 1 else rec.get('target_wdl')
                if k == 'target_win_draw_loss_away_win' and v is not None:
                    rec['target_wdl'] = 2 if int(v) == 1 else rec.get('target_wdl')
            if k in TARGET_FIELDS:
                continue
            else:
                rec[f'{pref}_{k}'] = v

    df = pd.DataFrame.from_dict(records, orient='index')

    # Оставляем только строки с валидным таргетом 0/1/2
    df = df[df['target_wdl'].isin([0, 1, 2])]

    y = df['target_wdl'].astype(int)
    X = df.drop(columns=[c for c in df.columns if c in ('target_wdl', 'match_id')], errors='ignore')
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors='coerce')
    X = X.fillna(0.0).astype(float)

    return X, y


def main() -> None:
    print("\n📊 Важность признаков для win_draw_loss (f_classif, MI)")
    print("=" * 72)
    X, y = load_dataset()
    if X.empty or y.empty:
        print('Нет данных для анализа')
        return

    f_vals, _ = f_classif(X, y)
    f_vals = np.nan_to_num(f_vals)

    mi_vals = mutual_info_classif(X, y, random_state=42)

    df_scores = pd.DataFrame({
        'feature': X.columns,
        'F_value': f_vals,
        'MI': mi_vals
    }).sort_values(by='MI', ascending=False)

    print(f"Объектов: {len(y):,}, Фич: {X.shape[1]:,}")
    print("\nТоп-30 по MI:")
    for _, r in df_scores.head(30).iterrows():
        print(f"  {r['feature']}: MI={r['MI']:.5f}, F={r['F_value']:.2f}")


if __name__ == '__main__':
    main()


