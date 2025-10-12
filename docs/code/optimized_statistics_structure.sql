-- Оптимизированная структура таблицы statistics без дублирования данных
-- Использует существующие таблицы outcomes, predictions, matchs

-- 1. ОСНОВНАЯ ТАБЛИЦА СТАТИСТИКИ (минимальная, без дублирования)
CREATE TABLE statistics_optimized (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    
    -- Связи с существующими таблицами (НЕ дублируем данные)
    outcome_id BIGINT NOT NULL,                -- Связь с outcomes (вместо дублирования прогнозных данных)
    prediction_id BIGINT,                      -- Связь с predictions (опционально)
    match_id BIGINT NOT NULL,                  -- Связь с matchs (для быстрых запросов)
    
    -- Дополнительная информация о контексте (НЕТ в других таблицах)
    championship_id BIGINT NOT NULL,           -- Чемпионат (нет прямой связи в outcomes)
    sport_id BIGINT NOT NULL,                  -- Вид спорта (нет прямой связи в outcomes)
    match_date DATE NOT NULL,                  -- Дата матча (для быстрых запросов)
    match_round VARCHAR(50),                   -- Раунд/тур (нет в других таблицах)
    match_stage VARCHAR(50),                    -- Стадия турнира (нет в других таблицах)
    
    -- Классификация прогноза (НЕТ в других таблицах)
    forecast_type VARCHAR(50) NOT NULL,        -- Тип прогноза (win_draw_loss, oz, goal_home, total_amount, etc.)
    forecast_subtype VARCHAR(50) NOT NULL,     -- Подтип (home_win, draw, away_win, both_score, over_2.5, etc.)
    
    -- Информация о модели (расширенная)
    model_name VARCHAR(100) NOT NULL,          -- Название модели
    model_version VARCHAR(20),                 -- Версия модели (нет в predictions)
    model_type VARCHAR(20),                    -- Тип модели (classification, regression)
    
    -- Результат матча (вычисляемые поля)
    actual_result VARCHAR(50),                -- Фактический результат (вычисляется из numOfHeads)
    actual_value DECIMAL(10,3),               -- Фактическое значение (для регрессии)
    
    -- Оценка качества прогноза (вычисляемые поля)
    prediction_correct BOOLEAN,                -- Правильность прогноза
    prediction_accuracy DECIMAL(5,4),         -- Точность прогноза (0.0000-1.0000)
    prediction_error DECIMAL(10,3),           -- Ошибка прогноза (для регрессии)
    prediction_residual DECIMAL(10,3),        -- Остаток (actual - predicted)
    
    -- Коэффициенты и доходность (НОВЫЕ поля)
    coefficient DECIMAL(8,3),                  -- Коэффициент ставки
    potential_profit DECIMAL(10,2),           -- Потенциальная прибыль
    actual_profit DECIMAL(10,2),              -- Фактическая прибыль
    
    -- Метаданные
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Индексы
    INDEX idx_outcome_id (outcome_id),
    INDEX idx_prediction_id (prediction_id),
    INDEX idx_match_id (match_id),
    INDEX idx_championship_id (championship_id),
    INDEX idx_sport_id (sport_id),
    INDEX idx_match_date (match_date),
    INDEX idx_forecast_type (forecast_type),
    INDEX idx_model_name (model_name),
    INDEX idx_prediction_correct (prediction_correct),
    INDEX idx_championship_date (championship_id, match_date),
    INDEX idx_sport_championship (sport_id, championship_id),
    
    -- Внешние ключи
    FOREIGN KEY (outcome_id) REFERENCES outcomes(id) ON DELETE CASCADE,
    FOREIGN KEY (prediction_id) REFERENCES predictions(id) ON DELETE SET NULL,
    FOREIGN KEY (match_id) REFERENCES matchs(id) ON DELETE CASCADE,
    FOREIGN KEY (championship_id) REFERENCES championships(id) ON DELETE CASCADE,
    FOREIGN KEY (sport_id) REFERENCES sports(id) ON DELETE CASCADE
);

-- 2. ТАБЛИЦА АГРЕГИРОВАННОЙ СТАТИСТИКИ (без изменений)
CREATE TABLE statistics_aggregated (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    
    -- Группировка
    group_type VARCHAR(50) NOT NULL,            -- Тип группировки (team, championship, model, etc.)
    group_id BIGINT NOT NULL,                   -- ID группы
    group_name VARCHAR(200),                    -- Название группы
    
    -- Временной период
    period_start DATE NOT NULL,                 -- Начало периода
    period_end DATE NOT NULL,                  -- Конец периода
    
    -- Фильтры
    sport_id BIGINT,                           -- Вид спорта
    championship_id BIGINT,                    -- Чемпионат
    forecast_type VARCHAR(50),                 -- Тип прогноза
    model_name VARCHAR(100),                    -- Модель
    
    -- Статистика
    total_predictions INT NOT NULL DEFAULT 0,   -- Общее количество прогнозов
    correct_predictions INT NOT NULL DEFAULT 0, -- Правильных прогнозов
    accuracy DECIMAL(5,4) NOT NULL DEFAULT 0, -- Точность (0.0000-1.0000)
    
    -- Для регрессии
    total_error DECIMAL(15,6) DEFAULT 0,       -- Суммарная ошибка
    mean_error DECIMAL(10,6) DEFAULT 0,       -- Средняя ошибка
    mse DECIMAL(15,6) DEFAULT 0,               -- Среднеквадратичная ошибка
    mae DECIMAL(10,6) DEFAULT 0,              -- Средняя абсолютная ошибка
    r2_score DECIMAL(5,4) DEFAULT 0,           -- R² коэффициент
    
    -- Доходность
    total_invested DECIMAL(15,2) DEFAULT 0,    -- Общая сумма ставок
    total_profit DECIMAL(15,2) DEFAULT 0,      -- Общая прибыль
    roi DECIMAL(8,4) DEFAULT 0,                -- ROI (Return on Investment)
    
    -- Метаданные
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Индексы
    INDEX idx_group_type_id (group_type, group_id),
    INDEX idx_period (period_start, period_end),
    INDEX idx_sport_championship (sport_id, championship_id),
    INDEX idx_forecast_model (forecast_type, model_name),
    
    -- Уникальность
    UNIQUE KEY unique_aggregation (
        group_type, group_id, period_start, period_end, 
        sport_id, championship_id, forecast_type, model_name
    )
);

-- 3. ТАБЛИЦА ДЕТАЛИЗАЦИИ МАТЧЕЙ (без изменений)
CREATE TABLE match_details (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    match_id BIGINT NOT NULL,
    
    -- Детали матча
    match_status VARCHAR(20),                 -- Статус матча (finished, cancelled, postponed)
    match_duration INT,                        -- Длительность матча (минуты)
    attendance INT,                            -- Посещаемость
    
    -- Детали по периодам
    period_1_home INT,                         -- Голы в 1-м периоде (дома)
    period_1_away INT,                         -- Голы в 1-м периоде (гости)
    period_2_home INT,                         -- Голы во 2-м периоде (дома)
    period_2_away INT,                         -- Голы во 2-м периоде (гости)
    
    -- Дополнительная информация
    extra_time BOOLEAN DEFAULT FALSE,          -- Было ли дополнительное время
    penalties BOOLEAN DEFAULT FALSE,           -- Были ли пенальти
    red_cards_home INT DEFAULT 0,             -- Красные карточки (дома)
    red_cards_away INT DEFAULT 0,             -- Красные карточки (гости)
    yellow_cards_home INT DEFAULT 0,          -- Желтые карточки (дома)
    yellow_cards_away INT DEFAULT 0,          -- Желтые карточки (гости)
    
    -- Погодные условия
    weather_condition VARCHAR(50),             -- Погодные условия
    temperature DECIMAL(5,2),                  -- Температура
    humidity DECIMAL(5,2),                     -- Влажность
    
    -- Метаданные
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Индексы
    INDEX idx_match_id (match_id),
    
    -- Внешние ключи
    FOREIGN KEY (match_id) REFERENCES matchs(id) ON DELETE CASCADE
);

-- 4. ТАБЛИЦА КОЭФФИЦИЕНТОВ (без изменений)
CREATE TABLE prediction_coefficients (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    statistics_id BIGINT NOT NULL,             -- Связь с основной статистикой
    
    -- Коэффициенты
    coefficient_value DECIMAL(8,3) NOT NULL,   -- Значение коэффициента
    coefficient_type VARCHAR(50),             -- Тип коэффициента (1x2, over_under, etc.)
    bookmaker VARCHAR(100),                   -- Букмекер
    
    -- Доходность
    stake_amount DECIMAL(10,2),               -- Размер ставки
    potential_win DECIMAL(10,2),              -- Потенциальный выигрыш
    actual_win DECIMAL(10,2),                 -- Фактический выигрыш
    
    -- Метаданные
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Индексы
    INDEX idx_statistics_id (statistics_id),
    INDEX idx_bookmaker (bookmaker),
    
    -- Внешние ключи
    FOREIGN KEY (statistics_id) REFERENCES statistics_optimized(id) ON DELETE CASCADE
);

-- 5. ПРЕДСТАВЛЕНИЕ ДЛЯ ПОЛНОЙ СТАТИСТИКИ (объединяет все таблицы)
CREATE VIEW statistics_full AS
SELECT 
    s.id,
    s.outcome_id,
    s.prediction_id,
    s.match_id,
    s.championship_id,
    s.sport_id,
    s.match_date,
    s.match_round,
    s.match_stage,
    s.forecast_type,
    s.forecast_subtype,
    s.model_name,
    s.model_version,
    s.model_type,
    s.actual_result,
    s.actual_value,
    s.prediction_correct,
    s.prediction_accuracy,
    s.prediction_error,
    s.prediction_residual,
    s.coefficient,
    s.potential_profit,
    s.actual_profit,
    s.created_at,
    s.updated_at,
    
    -- Данные из outcomes
    o.feature,
    o.forecast,
    o.outcome,
    o.probability,
    o.confidence,
    o.uncertainty,
    o.lower_bound,
    o.upper_bound,
    
    -- Данные из matchs
    m.teamHome_id,
    m.teamAway_id,
    m.gameData,
    m.numOfHeadsHome,
    m.numOfHeadsAway,
    m.gameComment,
    
    -- Данные из команд
    th.teamName as team_home_name,
    ta.teamName as team_away_name,
    
    -- Данные из чемпионата
    ch.championshipName,
    
    -- Данные из спорта
    sp.sportName

FROM statistics_optimized s
LEFT JOIN outcomes o ON s.outcome_id = o.id
LEFT JOIN matchs m ON s.match_id = m.id
LEFT JOIN teams th ON m.teamHome_id = th.id
LEFT JOIN teams ta ON m.teamAway_id = ta.id
LEFT JOIN championships ch ON s.championship_id = ch.id
LEFT JOIN sports sp ON s.sport_id = sp.id;
