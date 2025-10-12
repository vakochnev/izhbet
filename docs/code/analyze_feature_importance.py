#!/usr/bin/env python3
import logging
import numpy as np
import pandas as pd
from typing import Tuple

from sklearn.feature_selection import f_regression, mutual_info_regression

from config import Session_pool
from db.models import Feature
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


META = {'id', 'match_id', 'prefix', 'created_at', 'updated_at'}


def load_dataset(limit_matches: int = 5000) -> Tuple[pd.DataFrame, pd.Series]:
    with Session_pool() as db:
        # Соберём по одному матчу все 4 префикса и объединим в wide-формат
        match_ids = (
            db.query(Feature.match_id)
            .group_by(Feature.match_id)
            .limit(limit_matches)
            .all()
        )
        match_ids = [r[0] for r in match_ids]
        if not match_ids:
            return pd.DataFrame(), pd.Series(dtype=float)

        rows = (
            db.query(Feature)
            .filter(Feature.match_id.in_(match_ids))
            .all()
        )

    # Преобразуем в единый wide DF
    records = {}
    for r in rows:
        d = r.as_dict()
        mid = d['match_id']
        pref = d['prefix']
        rec = records.setdefault(mid, {'match_id': mid})
        for k, v in d.items():
            if k in META or k in DROP_FIELD_EMBEDDING or k.startswith('_'):
                continue
            # Оставим таргет для total_amount как отдельную колонку
            if k == 'target_total_amount' and pref == 'home':
                rec['target_total_amount'] = v
            elif k in TARGET_FIELDS:
                continue
            else:
                rec[f'{pref}_{k}'] = v

    df = pd.DataFrame.from_dict(records, orient='index')

    # Чистим и приводим типы
    y = pd.to_numeric(df['target_total_amount'], errors='coerce') if 'target_total_amount' in df.columns else pd.Series(dtype=float)
    X = df.drop(columns=[c for c in df.columns if c in ('target_total_amount', 'match_id')], errors='ignore')
    for c in X.columns:
        X[c] = pd.to_numeric(X[c], errors='coerce')
    X = X.fillna(0.0).astype(float)
    y = y.fillna(0.0).astype(float)

    return X, y


def main() -> None:
    print("\n📊 Важность признаков для target_total_amount (F, MI)")
    print("=" * 70)
    X, y = load_dataset()
    if X.empty or y.empty:
        print('Нет данных для анализа')
        return

    # F-statistic (линейная зависимость)
    f_vals, _ = f_regression(X, y)
    f_vals = np.nan_to_num(f_vals)

    # Mutual Information (нелинейная зависимость)
    mi_vals = mutual_info_regression(X, y, random_state=42)

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


