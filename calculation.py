# izhbet/calculation.py
from sys import argv
import logging
import pandas as pd

from core.constants import TIME_FRAME
from calculation.tournament import (
    CalculationDataPipeline, DatabaseSource, CreatingStandings,
    FileStorage
)

logger = logging.getLogger(__name__) #'unique.module.name'


def main():
    """
    Основная функция модуля. Запускает процесс расчета параметров турнирной таблицы.
    """
    logger.info('Запущен модуль расчета параметров турнирной таблицы.')
    try:
        time_frame = argv[1]
    except IndexError:
        time_frame = 'LATELY'
    else:
        if time_frame not in TIME_FRAME:
            time_frame = 'LATELY'

    # Создавайте экземпляры конкретных классов
    data_source = DatabaseSource()
    data_processor = CreatingStandings()
    data_storage = FileStorage()

    # Создайте экземпляр класса конвейера передачи данных
    pipeline = CalculationDataPipeline(
        data_source,
        data_processor,
        data_storage
    )

    # Определите метод select_data в классе конвейера передачи данных
    def select_data(self, tournament_id):

        df_match_tournament = (self.data_source.df[
            self.data_source.df['tournament_id'] == tournament_id].
            sort_values(by='gameData', ascending=True)
        )

        df_team = (
            self.data_source.df[['teamHome_id', 'teamAway_id']]
                [self.data_source.df['tournament_id'] == tournament_id]
        )

        df_team_tournament = pd.concat(
            objs=[df_team['teamHome_id'], df_team['teamAway_id']],
            axis=0
        )

        df_team_tournament = df_team_tournament.unique()

        return df_team_tournament, df_match_tournament

    CalculationDataPipeline.select_data = select_data

    # Запустите конвейер (внутри теперь формируются snapshots)
    pipeline.process_data(time_frame)

    # После обработки экспортируем агрегированный индекс snapshot-файлов
    # try:
    #     import os, json, glob
    #     out_dir = 'results/standings_snapshots'
    #     if os.path.isdir(out_dir):
    #         files = sorted(glob.glob(os.path.join(out_dir, '*.csv')))
    #         index_path = os.path.join(out_dir, 'index.json')
    #         meta = {
    #             'generated_files': [os.path.basename(p) for p in files],
    #             'count': len(files)
    #         }
    #         with open(index_path, 'w', encoding='utf-8') as f:
    #             json.dump(meta, f, ensure_ascii=False, indent=2)
    #         logger.info(f'Экспорт snapshot-индекса: {index_path}')
    # except Exception as e:
    #     logger.error(f'Ошибка экспорта snapshot-индекса: {e}')

    logger.info(
        'Завершил работу модуль расчета параметров '
        'турнирной таблицы.'
    )


if __name__ == '__main__':
    main()