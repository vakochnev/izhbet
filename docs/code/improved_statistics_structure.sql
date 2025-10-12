-- Улучшенная структура таблицы statistics для анализа прогнозов
-- с детализацией матчей и поддержкой фронтенда

-- 1. ОСНОВНАЯ ТАБЛИЦА СТАТИСТИКИ (заменяет текущую)
CREATE TABLE statistics_improved (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    
    -- Связи с основными сущностями
    match_id BIGINT NOT NULL,                    -- Связь с конкретным матчем
    team_home_id BIGINT NOT NULL,                -- Домашняя команда
    team_away_id BIGINT NOT NULL,                -- Гостевая команда
    championship_id BIGINT NOT NULL,             -- Чемпионат
    tournament_id BIGINT NOT NULL,               -- Турнир/сезон
    sport_id BIGINT NOT NULL,                    -- Вид спорта
    
    -- Информация о матче
    match_date DATETIME NOT NULL,                -- Дата матча
    match_round VARCHAR(50),                    -- Раунд/тур
    match_stage VARCHAR(50),                    -- Стадия турнира
    
    -- Прогнозная информация
    forecast_type VARCHAR(50) NOT NULL,         -- Тип прогноза (win_draw_loss, oz, goal_home, total_amount, etc.)
    forecast_subtype VARCHAR(50) NOT NULL,      -- Подтип (home_win, draw, away_win, both_score, over_2.5, etc.)
    forecast_value DECIMAL(10,3),               -- Значение прогноза (для регрессии)
    forecast_probability DECIMAL(5,4),          -- Вероятность прогноза (0.0000-1.0000)
    forecast_confidence DECIMAL(5,4),           -- Уверенность модели (0.0000-1.0000)
    forecast_uncertainty DECIMAL(5,4),          -- Неопределенность (0.0000-1.0000)
    
    -- Интервалы для конформных прогнозов
    forecast_lower_bound DECIMAL(10,3),         -- Нижняя граница интервала
    forecast_upper_bound DECIMAL(10,3),         -- Верхняя граница интервала
    
    -- Информация о модели
    model_name VARCHAR(100) NOT NULL,           -- Название модели
    model_version VARCHAR(20),                  -- Версия модели
    model_type VARCHAR(20),                     -- Тип модели (classification, regression)
    
    -- Результат матча
    actual_result VARCHAR(50),                  -- Фактический результат (home_win, draw, away_win, etc.)
    actual_value DECIMAL(10,3),                 -- Фактическое значение (для регрессии)
    goal_home INT,                              -- Голы домашней команды
    goal_away INT,                              -- Голы гостевой команды
    
    -- Оценка качества прогноза
    prediction_correct BOOLEAN,                 -- Правильность прогноза
    prediction_accuracy DECIMAL(5,4),          -- Точность прогноза (0.0000-1.0000)
    prediction_error DECIMAL(10,3),            -- Ошибка прогноза (для регрессии)
    prediction_residual DECIMAL(10,3),         -- Остаток (actual - predicted)
    
    -- Коэффициенты и доходность
    coefficient DECIMAL(8,3),                   -- Коэффициент ставки
    potential_profit DECIMAL(10,2),            -- Потенциальная прибыль
    actual_profit DECIMAL(10,2),               -- Фактическая прибыль
    
    -- Метаданные
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Индексы
    INDEX idx_match_id (match_id),
    INDEX idx_championship_id (championship_id),
    INDEX idx_tournament_id (tournament_id),
    INDEX idx_sport_id (sport_id),
    INDEX idx_match_date (match_date),
    INDEX idx_forecast_type (forecast_type),
    INDEX idx_model_name (model_name),
    INDEX idx_prediction_correct (prediction_correct),
    INDEX idx_championship_date (championship_id, match_date),
    INDEX idx_sport_championship (sport_id, championship_id),
    
    -- Внешние ключи
    FOREIGN KEY (match_id) REFERENCES matchs(id) ON DELETE CASCADE,
    FOREIGN KEY (team_home_id) REFERENCES teams(id) ON DELETE CASCADE,
    FOREIGN KEY (team_away_id) REFERENCES teams(id) ON DELETE CASCADE,
    FOREIGN KEY (championship_id) REFERENCES championships(id) ON DELETE CASCADE,
    FOREIGN KEY (tournament_id) REFERENCES tournaments(id) ON DELETE CASCADE,
    FOREIGN KEY (sport_id) REFERENCES sports(id) ON DELETE CASCADE
);

-- 2. ТАБЛИЦА АГРЕГИРОВАННОЙ СТАТИСТИКИ (для быстрых запросов)
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

-- 3. ТАБЛИЦА ДЕТАЛИЗАЦИИ МАТЧЕЙ (для подробного анализа)
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

-- 4. ТАБЛИЦА КОЭФФИЦИЕНТОВ (для анализа доходности)
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
    FOREIGN KEY (statistics_id) REFERENCES statistics_improved(id) ON DELETE CASCADE
);

