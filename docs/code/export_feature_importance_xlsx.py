#!/usr/bin/env python3
import logging
import os
import pandas as pd
from datetime import datetime
import glob

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def load_and_sort_csv(csv_path: str, target_name: str) -> pd.DataFrame:
    if not os.path.exists(csv_path):
        return pd.DataFrame()
    df = pd.read_csv(csv_path)
    df = df.sort_values('MI', ascending=False)
    df['target'] = target_name
    return df


def export_unified_xlsx() -> str:
    out_dir = os.path.join('results', 'quality', 'feature_importance')
    os.makedirs(out_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = os.path.join(out_dir, f'unified_feature_importance_{ts}.xlsx')

    targets = [
        ('win_draw_loss', 'Win_Draw_Loss'),
        ('oz', 'Both_Score'),
        ('total_amount', 'Total_Amount'),
        ('total_home_amount', 'Total_Home_Amount'),
        ('total_away_amount', 'Total_Away_Amount')
    ]

    with pd.ExcelWriter(out_path, engine='openpyxl') as writer:
        for target_key, sheet_name in targets:
            csv_path = os.path.join(out_dir, f'{target_key}_*.csv')
            # Найдём последний файл с этим префиксом
            files = glob.glob(csv_path)
            if files:
                latest_file = max(files, key=os.path.getctime)
                df = load_and_sort_csv(latest_file, target_key)
                if not df.empty:
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    print(f"✅ {sheet_name}: {len(df)} фич")
                else:
                    print(f"❌ {sheet_name}: нет данных")
            else:
                print(f"❌ {sheet_name}: файл не найден")

    return out_path


if __name__ == '__main__':
    out_path = export_unified_xlsx()
    print(f"\n📊 Единый файл: {out_path}")
