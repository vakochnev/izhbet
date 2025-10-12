#!/bin/bash

cd /home/wka/code/izhbet

source venv/bin/activate

# docker exec -i $CONTAINER_NAME mysql -u$DATABASE_USER -p$DATABASE_PASSWORD $DATABASE_NAME --max_allowed_packet=1073741824 -e "SET GLOBAL FOREIGN_KEY_CHECKS = 0;"
mysql --host=$DATABASE_HOST --user=$DATABASE_USER --password=$DATABASE_PASSWORD --max_allowed_packet=1073741824 -e "SET GLOBAL FOREIGN_KEY_CHECKS = 0;"

if [ $? -eq 0 ]; then
    echo "FOREIGN_KEY_CHECKS успешно отключен"
else
    echo "Ошибка при отключении FOREIGN_KEY_CHECKS..."
fi

# Получение данных с сайта stat-api.baltbet.ru и сохранение в БД
# можно передать параметры: INIT_DB, UPDATE_DB - по умолчанию UPDATE_DB
python3.12 getting.py UPDATE_DB -Wignore
if [ $? -ne 0 ]; then
    echo "Программа: getting.py - завершилась с ошибкой" >&2
    exit $?
fi

# Расчет турнирной таблицы и эмбедингов для нейронной сети
# можно передать параметры ALL_TIME, LATELY - по умолчанию LATELY
python3.12 calculation.py LATELY -Wignore
if [ $? -ne 0 ]; then
    echo "Программа: calculation.py - завершилась с ошибкой" >&2
    exit $?
fi

# Построение модели и предсказания результатов и сохранение их в таблицу
# можно передать параметры CREATE_MODEL, CREATE_PROGNOZ - по умолчанию CREATE_PROGNOZ
python3.12 processing.py CREATE_PROGNOZ -Wignore
if [ $? -ne 0 ]; then
    echo "Программа: processing.py - завершилась с ошибкой" >&2
    exit $?
fi

# Публикация наиболее вероятных событий в соц.сети
python3.12 forecast.py all_time
if [ $? -ne 0 ]; then
    echo "Программа: publisher.py - завершилась с ошибкой" >&2
    exit $?
fi

exit 0

# Публикация наиболее вероятных событий в соц.сети
python3.12 publisher.py TODAY -Wignore
if [ $? -ne 0 ]; then
    echo "Программа: publisher.py - завершилась с ошибкой" >&2
    exit $?
fi

deactivate
