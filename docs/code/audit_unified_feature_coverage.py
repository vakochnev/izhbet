#!/usr/bin/env python3
import logging
from collections import defaultdict
from typing import Dict, List

import pandas as pd

from config import Session_pool
from db.models import Feature
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


META = {'id', 'match_id', 'prefix', 'created_at', 'updated_at'}


def fetch_match_ids(limit_matches: int = 1000) -> List[int]:
    with Session_pool() as db:
        rows = (
            db.query(Feature.match_id)
            .group_by(Feature.match_id)
            .limit(limit_matches)
            .all()
        )
        return [r[0] for r in rows]


def row_to_features(row: dict) -> Dict[str, float]:
    pref = row['prefix']
    result = {}
    for k, v in row.items():
        if k in META or k in TARGET_FIELDS or k in DROP_FIELD_EMBEDDING or k.startswith('_'):
            continue
        result[f"{pref}_{k}"] = pd.to_numeric(v, errors='coerce')
    return result


def assemble_for_match(match_id: int) -> Dict[str, float]:
    with Session_pool() as db:
        rows = (
            db.query(Feature)
            .filter(Feature.match_id == match_id, Feature.prefix.in_(['home','away','diff','ratio']))
            .all()
        )
        combined: Dict[str, float] = {}
        for r in rows:
            combined.update(row_to_features(r.as_dict()))
        return combined


def audit(limit_matches: int = 1000) -> None:
    print("\n🔎 Проверка покрытия объединённого вектора фич")
    print("=" * 60)
    match_ids = fetch_match_ids(limit_matches)
    if not match_ids:
        print('Нет match_id в features')
        return

    records: List[Dict[str, float]] = []
    for mid in match_ids:
        rec = assemble_for_match(mid)
        rec['match_id'] = mid
        records.append(rec)

    df = pd.DataFrame(records)
    total_cols = df.shape[1] - 1  # exclude match_id
    print(f"Матчей проверено: {len(df):,}")
    print(f"Число колонок в объединённом векторе: {total_cols:,}")

    miss_pct = df.drop(columns=['match_id']).isna().mean()
    print(f"Средняя доля пропусков по колонкам: {miss_pct.mean():.2%}")

    per_match_missing = df.drop(columns=['match_id']).isna().sum(axis=1)
    print(f"Медиана пропусков на матч: {int(per_match_missing.median())}")
    print(f"P95 пропусков на матч: {int(per_match_missing.quantile(0.95))}")

    print("\nТоп-20 колонок по пропускам:")
    top = miss_pct.sort_values(ascending=False).head(20)
    for k, v in top.items():
        print(f"  {k}: {v:.2%}")


if __name__ == '__main__':
    audit()


