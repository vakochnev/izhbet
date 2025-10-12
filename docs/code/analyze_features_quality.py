#!/usr/bin/env python3
import logging
from typing import List, Tuple

import pandas as pd

from config import Session_pool
from db.models import Feature
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


NON_FEATURE_COLUMNS = {
    'id', 'match_id', 'prefix', 'created_at', 'updated_at'
}


def _load_features(limit: int = 10000) -> pd.DataFrame:
    with Session_pool() as db:
        rows: List[Feature] = db.query(Feature).limit(limit).all()
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame([r.as_dict() for r in rows])


def _prepare_feature_df(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    if df.empty:
        return df, []

    # Drop non-feature and target columns
    to_drop = set(TARGET_FIELDS) | set(DROP_FIELD_EMBEDDING) | NON_FEATURE_COLUMNS
    existing = [c for c in to_drop if c in df.columns]
    df_feat = df.drop(columns=existing, errors='ignore')

    # Keep only numeric columns and coerce
    for col in df_feat.columns:
        df_feat[col] = pd.to_numeric(df_feat[col], errors='coerce')

    # Return list of feature columns
    feature_cols = list(df_feat.columns)
    return df_feat, feature_cols


def analyze_quality(limit: int = 10000) -> None:
    print("\n🔍 АНАЛИЗ КАЧЕСТВА ФИЧЕЙ")
    print("=" * 60)

    df = _load_features(limit=limit)
    if df.empty:
        print("Нет данных в таблице features")
        return

    total_rows = len(df)
    prefixes = df['prefix'].value_counts(dropna=False) if 'prefix' in df.columns else None

    df_feat, feature_cols = _prepare_feature_df(df)
    if df_feat.empty:
        print("Не удалось подготовить фичи (пустой датафрейм после очистки)")
        return

    # Basic stats
    num_features = len(feature_cols)
    print(f"Строк (feature-объектов): {total_rows:,}")
    print(f"Количество признаков (столбцов): {num_features:,}")

    # Missing rates
    missing_pct = df_feat.isna().mean().sort_values(ascending=False)
    zero_var = (df_feat.nunique(dropna=False) <= 1)
    all_zero = (df_feat.fillna(0).sum(axis=0) == 0)

    num_missing_gt50 = int((missing_pct > 0.5).sum())
    num_zero_var = int(zero_var.sum())
    num_all_zero = int(all_zero.sum())

    print(f"Колонок с пропусками > 50%: {num_missing_gt50}")
    print(f"Константных колонок (нулевая дисперсия): {num_zero_var}")
    print(f"Колонок, сумма=0: {num_all_zero}")

    # Per-prefix coverage
    if prefixes is not None:
        print("\nПокрытие по prefix:")
        for p, cnt in prefixes.items():
            print(f"  {p}: {cnt:,}")

    # Top problem columns
    top_missing = missing_pct.head(20)
    if not top_missing.empty:
        print("\nТоп-20 колонок по доле пропусков:")
        for name, val in top_missing.items():
            print(f"  {name}: {val:.2%}")

    if num_zero_var > 0:
        print("\nПримеры константных колонок:")
        for name in zero_var[zero_var].index[:20]:
            print(f"  {name}")

    # Feature count per match expectation
    if 'match_id' in df.columns:
        cols_per_prefix = df.groupby('prefix')['match_id'].nunique() if 'prefix' in df.columns else None
        unique_matches = df['match_id'].nunique()
        print(f"\nУникальных матчей в выборке: {unique_matches:,}")
        if cols_per_prefix is not None:
            print("Уникальных матчей по prefix:")
            for p, v in cols_per_prefix.items():
                print(f"  {p}: {int(v):,}")

    print("\nРЕКОМЕНДАЦИИ:")
    print("- Проверьте колонки с высокой долей пропусков (>50%) и константные")
    print("- Убедитесь, что формируется ожидаемое число фичей на матч (около 2160)")
    print("- Проверьте баланс по prefix ('home','away','diff','ratio')")


if __name__ == '__main__':
    analyze_quality()


