#!/usr/bin/env python3
import logging
from typing import Dict, List, Set

from config import Session_pool
from db.models import Feature
from core.constants import TARGET_FIELDS, DROP_FIELD_EMBEDDING

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


META_FIELDS = {'id', 'match_id', 'prefix', 'created_at', 'updated_at'}


def get_feature_attr_names(f: Feature) -> List[str]:
    names: List[str] = []
    for k, v in f.__dict__.items():
        if k.startswith('_'):
            continue
        if k in META_FIELDS:
            continue
        if k in TARGET_FIELDS:
            continue
        if k in DROP_FIELD_EMBEDDING:
            continue
        names.append(k)
    return names


def main() -> None:
    with Session_pool() as db:
        # Найдём любой match_id у которого есть все 4 префикса
        row = db.query(Feature.match_id).group_by(Feature.match_id).first()
        if not row:
            print('features пусто')
            return
        match_id = row[0]

        feats_by_prefix: Dict[str, Feature] = {}
        for p in ('home', 'away', 'diff', 'ratio'):
            feats_by_prefix[p] = (
                db.query(Feature)
                .filter(Feature.match_id == match_id, Feature.prefix == p)
                .first()
            )

        # Собираем имена
        unified: List[str] = []
        missing_prefixes: List[str] = []
        for p, inst in feats_by_prefix.items():
            if not inst:
                missing_prefixes.append(p)
                continue
            base_names = get_feature_attr_names(inst)
            unified.extend([f"{p}_{n}" for n in base_names])

        # Убираем дубликаты, сохраняем порядок
        seen: Set[str] = set()
        uniq: List[str] = []
        for n in unified:
            if n not in seen:
                seen.add(n)
                uniq.append(n)

        if missing_prefixes:
            print(f"Отсутствуют prefix: {', '.join(missing_prefixes)}")

        print(f"Всего фичей в едином векторе: {len(uniq)}")
        for name in uniq:
            print(name)


if __name__ == '__main__':
    main()


