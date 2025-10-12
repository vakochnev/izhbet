#!/bin/bash

cd /home/wka/code/izhbet

source venv/bin/activate

echo "=========================================="
echo "🚀 ЗАПУСК ИНТЕГРИРОВАННОГО ПАЙПЛАЙНА"
echo "=========================================="

# Отключаем проверку внешних ключей для ускорения работы
mysql --host=$DATABASE_HOST --user=$DATABASE_USER --password=$DATABASE_PASSWORD --max_allowed_packet=1073741824 -e "SET GLOBAL FOREIGN_KEY_CHECKS = 0;"

if [ $? -eq 0 ]; then
    echo "✅ FOREIGN_KEY_CHECKS успешно отключен"
else
    echo "❌ Ошибка при отключении FOREIGN_KEY_CHECKS..."
fi

echo ""
echo "📊 ЭТАП 1: Получение данных с сайта stat-api.baltbet.ru"
echo "=================================================="
python3.12 getting.py UPDATE_DB -Wignore
if [ $? -ne 0 ]; then
    echo "❌ Программа: getting.py - завершилась с ошибкой" >&2
    exit $?
fi
echo "✅ Данные успешно получены и сохранены в БД"

echo ""
echo "🧮 ЭТАП 2: Расчет турнирной таблицы"
echo "=============================================="
python3.12 calculation.py LATELY -Wignore
if [ $? -ne 0 ]; then
    echo "❌ Программа: calculation.py - завершилась с ошибкой" >&2
    exit $?
fi
echo "✅ Турнирные таблицы и snapshots рассчитаны"

echo ""
echo "🎯 ЭТАП 3-5: ИНТЕГРИРОВАННЫЙ ПАЙПЛАЙН"
echo "====================================="
echo "  🔧 Processing → Forecast → Publisher"
echo ""

# Запускаем интегрированный пайплайн
python3.12 run_pipeline.py today --verbose
if [ $? -ne 0 ]; then
    echo "❌ Интегрированный пайплайн завершился с ошибкой" >&2
    exit $?
fi

echo ""
echo "=========================================="
echo "🎉 ИНТЕГРИРОВАННЫЙ ПАЙПЛАЙН ЗАВЕРШЕН!"
echo "=========================================="
echo "📊 Результаты:"
echo "  • Данные получены и обработаны"
echo "  • Турнирные таблицы рассчитаны"
echo "  • Модели обучены и прогнозы созданы"
echo "  • Конформное прогнозирование выполнено"
echo "  • Качественные прогнозы отобраны"
echo "  • Результаты опубликованы"
echo "=========================================="

deactivate
