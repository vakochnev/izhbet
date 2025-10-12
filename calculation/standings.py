# izhbet/calculation/standings.py
"""
Модуль реализации классов для работы со статистикой команд и турнирами.
"""
from datetime import datetime
import logging
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod

from core.constants import SIZE_TOTAL, SIZE_ITOTAL, POWER


logger = logging.getLogger(__name__)


class Tournament:
    """
    Паттерн Singleton для хранения состояния турнира.

    Attributes:
        _instance: Экземпляр класса (singleton)
        teams: Словарь команд в турнире
    """
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Tournament, cls).__new__(cls)
            cls._instance.teams = {}
        return cls._instance

    @classmethod
    def clean(cls):
        """
        Очистка экземпляра класса.
        """
        cls._instance = None

    def add_team(self, team_name):
        """
        Добавление команды в турнир

        Args:
            team_name: Название команды
        """
        if team_name not in self.teams:
            self.teams[team_name] = Team(team_name)

    def get_team(self, team_name):
        """
        Получение команды по названию

        Args:
            team_name: Название команды
        Returns:
            Team: Объект команды или None, если команда не найдена
        """
        return self.teams.get(team_name)

    def add_match(self,
        match_id: int,
        sport_id: int,
        country_id: int,
        tournament_id: int,
        game_data: datetime,
        home_team_name,
        away_team_name,
        home_goals: int,
        away_goals: int,
        overtime: str,
        season_id: int,
        stages_id: int,
    ):
        """
        Добавление матча в турнир

        Args:
            match_id: ID матча
            sport_id: ID вида спорта
            country_id: ID страны
            tournament_id: ID турнира
            game_data: Дата матча
            home_team_name: Название домашней команды
            away_team_name: Название гостевой команды
            home_goals: Голы домашней команды
            away_goals: Голы гостевой команды
            overtime: Информация о дополнительном времени
            season_id: ID сезона
            stages_id: ID этапа
        """
        home_team = self.get_team(home_team_name)
        away_team = self.get_team(away_team_name)

        if home_team and away_team:
            match = Match(
                match_id, sport_id, country_id, tournament_id,
                game_data, home_team, away_team, home_goals,
                away_goals, overtime, season_id, stages_id
            )
            match.play()

    @staticmethod
    def calculate_ratings(table_strategy):
        """
        Расчет рейтингов на основе стратегии

        Args:
            table_strategy: Стратегия расчета рейтингов
        """
        table_strategy.calculate_ratings()


class RatingStrategy(ABC):
    """
    Паттерн Strategy для расчета рейтингов на основе
    отфильтрованных матчей.
    Абстрактный класс.
    """
    def calculate_ratings(self, filtered_teams):
        """
        Расчет рейтингов для отфильтрованных команд

        Args:
            filtered_teams: Список отфильтрованных команд
        """
        raise NotImplementedError


class DIFRatingStrategy(RatingStrategy):
    """
    Реализация расчета DIF-рейтинга.
    На вход поступает фильтрованный список команд.
    Для реализации формирование различных турнирных таблиц:
     - только Домашние игры
     - Гостевые игры
     - только Сильные соперники
     - Слабые соперники
     - Средние соперники
    """
    def calculate_ratings(self, filtered_teams):
        """
        Расчет DIF-рейтинга для команд

        Args:
            filtered_teams: Список отфильтрованных команд
        """
        n = len(filtered_teams)
        a = np.zeros((n, n))
        b = np.zeros(n)

        team_name_to_index = {
            team.name: i for i, team in enumerate(filtered_teams)
        }

        for team in filtered_teams:
            team_index = team_name_to_index[team.name]
            lost_matches = [
                match for match in team.filtered_matches
                    if match[2] == 'loss'
            ]
            b[team_index] = abs(
                sum((match[0] - match[1]) for match in lost_matches)
            )
            for match in lost_matches:
                opponent = match[4]
                # Проверяем, есть ли соперник в списке команд
                if opponent.name in team_name_to_index:
                    opponent_index = team_name_to_index[opponent.name]
                    # Разница голов в матче
                    a[team_index, opponent_index] = match[0] - match[1]

        a += np.eye(n)
        if np.linalg.det(a) == 0:
            # logger.error('Матрица A необратима, возможно, недостаточно '
            #       'данных для решения.')
            return

        try:
            x = np.linalg.solve(a, b)
            for i, team in enumerate(filtered_teams):
                team.dif_rating = x[i]
        except Exception:
            pass


class VORatingStrategy(RatingStrategy):
    """
    Реализация расчета VO-рейтинга.
    Рейтинг взвешанных очков
    На вход поступает фильтрованный список команд.
    Для реализации формирование различных турнирных таблиц:
     - только Домашние игры
     - Гостевые игры
     - только Сильные соперники
     - Слабые соперники
     - Средние соперники
    """
    def calculate_ratings(self, filtered_teams):
        """
        Расчет VO-рейтинга для команд

        Args:
            filtered_teams: Список отфильтрованных команд
        """
        total_points = sum(team.points for team in filtered_teams)
        total_vo_score = 0
        for team in filtered_teams:
            vo_score = sum(
                opponent.points for match in team.filtered_matches
                    if match[2] == 'win' for opponent in [match[4]]
            )
            vo_score += 0.5 * sum(
                opponent.points for match in team.filtered_matches
                    if match[2] == 'draw' for opponent in [match[4]]
            )
            team.vo_rating = vo_score
            total_vo_score += vo_score
        if total_vo_score > 0:
            weight_coeff = total_points / total_vo_score
            for team in filtered_teams:
                try:
                    team.vo_rating /= weight_coeff
                except ZeroDivisionError:
                    pass


class ELORatingStrategy(RatingStrategy):
    """Реализация расчета ЭЛО-рейтинга"""

    def calculate_ratings(self, filtered_teams):
        """
        Расчет ЭЛО-рейтинга для команд

        Args:
            filtered_teams: Список отфильтрованных команд
        """
        for team in filtered_teams:
            for match in team.filtered_matches:
                match_id = match[5]
                home_team = team
                away_team = match[4]
                home_goals, away_goals = match[0], match[1]
                if home_team.name in self.get_strong_teams(
                        filtered_teams
                ):
                    # Сильный соперник
                    opponent_strength = 1
                elif home_team.name in self.get_medium_teams(
                        filtered_teams
                ):
                    # Средний соперник
                    opponent_strength = 0.5
                else:
                    # Слабый соперник
                    opponent_strength = 0
                # Обновление ЭЛО-рейтингов для домашней и гостевой команд
                if home_goals > away_goals:
                    home_result, away_result = 1, 0
                elif home_goals < away_goals:
                    home_result, away_result = 0, 1
                else:
                    home_result, away_result = 0.5, 0.5
                home_team.elo_update(
                    away_team.elo_rating,
                    home_result,
                    is_home_game=True,
                    opponent_strength=opponent_strength
                )
                away_team.elo_update(
                    home_team.elo_rating,
                    away_result,
                    is_home_game=False,
                    opponent_strength=opponent_strength
                )

    @staticmethod
    def get_strong_teams(teams):
        """
        Получение списка сильных команд

        Args:
            teams: Список команд

        Returns:
            list: Верхняя треть команд
        """
        sorted_teams = sorted(teams, key=lambda x: -x.points)
        return sorted_teams[:len(teams) // 3]

    @staticmethod
    def get_medium_teams(teams):
        """
        Получение списка средних команд

        Args:
            teams: Список команд

        Returns:
            list: Средняя треть команд
        """
        sorted_teams = sorted(teams, key=lambda x: -x.points)
        return sorted_teams[len(teams) // 3:2 * len(teams) // 3]

    @staticmethod
    def get_weak_teams(teams):
        """
        Получение списка слабых команд

        Args:
            teams: Список команд

        Returns:
            list: Нижняя треть команд
        """
        sorted_teams = sorted(teams, key=lambda x: -x.points)
        return sorted_teams[2 * len(teams) // 3:]


class PotemkinRatingStrategy(RatingStrategy):
    """Реализация расчета "Потёмкин-рейтинга" """

    def calculate_ratings(self, filtered_teams):
        """
        Расчет "Потёмкин-рейтинга" для команд

        Args:
            filtered_teams: Список отфильтрованных команд
        """
        for team in filtered_teams:
            for match in team.filtered_matches:
                # Текущая команда рассматривается как away team
                home_team = team
                # Соперник домашней команды (away team)
                away_team = match[4]

                home_goals, away_goals = match[0], match[1]

                # Ставка каждой команды — 10% от её текущего "Потёмкин-рейтинга"
                home_stake = home_team.potemkin_rating * 0.1
                away_stake = away_team.potemkin_rating * 0.1
                # Общий фонд матча
                total_stake = home_stake + away_stake

                # Распределение фонда по забитым голам
                if home_goals + away_goals > 0:
                    home_share = (
                            home_goals / (home_goals + away_goals) *
                            total_stake
                    )
                    away_share = (
                            away_goals / (home_goals + away_goals) *
                            total_stake
                    )
                else:
                    home_share = away_share = total_stake / 2

                # Обновляем "Потёмкин-рейтинг"
                home_team.potemkin_rating += home_share - home_stake
                away_team.potemkin_rating += away_share - away_stake


class PowerRatingStrategy(RatingStrategy):
    """Реализация расчета рейтинга силы"""

    def calculate_ratings(self, filtered_teams):
        """
        Расчет рейтинга силы для команд

        Args:
            filtered_teams: Список отфильтрованных команд
        """
        for team in filtered_teams:
            for match in team.filtered_matches:
                # Текущая команда рассматривается как away team
                home_team = team
                # Соперник домашней команды (away team)
                away_team = match[4]

                home_goals, away_goals = match[0], match[1]
                if home_goals > 9:
                    home_goals = 9
                if away_goals > 9:
                    away_goals = 9

                home_power = POWER[
                    str(int(home_goals)) + str(int(away_goals))
                ]
                away_power = POWER[
                    str(int(away_goals)) + str(int(home_goals))
                ]
                home_team.power_rating += home_power
                away_team.power_rating += away_power


class Team:
    """
    Класс команды с расчетами статистики
        name - Название команды
        games_played - Количество сыграно игр
        games_wins - Количество выигранных игр
        games_draws - Количество игр сыгранных вничью
        games_losses - Количество проигранных игр
        overtime_wins - Количество игр выигранных в ОТ
        overtime_losses - Количество игр проигранных в ОТ
        goals_scored - Количество забитых голов
        goals_conceded - Количество пропущеных голов
        points - Количество набранных очков
        tb_points - Количество матчей с ТБ
            (2.5 - Футбол, 4.5 - Хоккей)
        tm_points - Количество матчей с ТМ
        itb_points - Количество матчей с ИТБ
            (1.5 - Футбол, 2.5 - Хоккей)
        itm_points - Количество матчей с ИТМ
        tb05_points - Количество матчей с ТБ 0.5
        tb15_points - Количество матчей с ТБ 1.5
        tb25_points - Количество матчей с ТБ 2.5
        tb35_points - Количество матчей с ТБ 3.5
        tb45_points - Количество матчей с ТБ 4.5
        tb55_points - Количество матчей с ТБ 5.5
        itb05_points - Количество матчей с ИТБ 0.5
        itb15_points - Количество матчей с ИТБ 1.5
        itb25_points - Количество матчей с ИТБ 2.5
        itb35_points - Количество матчей с ИТБ 3.5
        itb45_points - Количество матчей с ИТБ 4.5
        itb55_points - Количество матчей с ИТБ 5.5
        tm05_points - Количество матчей с ИТМ 0.5
        tm15_points - Количество матчей с ИТМ 1.5
        tm25_points - Количество матчей с ИТМ 2.5
        tm35_points - Количество матчей с ИТМ 3.5
        tm45_points - Количество матчей с ИТМ 4.5
        tm55_points - Количество матчей с ИТМ 5.5
        itm05_points - Количество матчей с ТМ 0.5
        itm15_points - Количество матчей с ТМ 1.5
        itm25_points - Количество матчей с ТМ 2.5
        itm35_points - Количество матчей с ТМ 3.5
        itm45_points - Количество матчей с ТМ 4.5
        itm55_points - Количество матчей с ТМ 5.5
        oz_points - Количество матчей и исходом ОЗ
        ozn_points - Количество матчей и исходом ОЗН
        victory_dry - Количество матчей победила не пропустив
        lossing_dry - Количество матчей проиграла не забив
        elo_rating - ЭЛО рейтинг
        vo_rating - ВО-оценка силы
        dif_rating - DIF-рейтинг
        potemkin_rating - "Потёмкин-рейтинг"
        power_rating - Рейтинг силы
        matches - Все матчи
        filtered_matches - Для хранения фильтрованных матчей
    """
    def __init__(self, name: str):
        self.name = name
        self.games_played = 0
        self.games_wins = 0
        self.games_draws = 0
        self.games_losses = 0
        self.overtime_wins = 0
        self.overtime_losses = 0
        self.goals_scored = 0
        self.goals_conceded = 0
        self.points = 0
        self.tb_points = 0
        self.tm_points = 0
        self.itb_points = 0
        self.itm_points = 0
        self.tb05_points = 0
        self.tb15_points = 0
        self.tb25_points = 0
        self.tb35_points = 0
        self.tb45_points = 0
        self.tb55_points = 0
        self.itb05_points = 0
        self.itb15_points = 0
        self.itb25_points = 0
        self.itb35_points = 0
        self.itb45_points = 0
        self.itb55_points = 0
        self.tm05_points = 0
        self.tm15_points = 0
        self.tm25_points = 0
        self.tm35_points = 0
        self.tm45_points = 0
        self.tm55_points = 0
        self.itm05_points = 0
        self.itm15_points = 0
        self.itm25_points = 0
        self.itm35_points = 0
        self.itm45_points = 0
        self.itm55_points = 0
        self.oz_points = 0
        self.ozn_points = 0
        self.victory_dry = 0
        self.lossing_dry = 0
        self.elo_rating = 1500
        self.vo_rating = 0
        self.dif_rating = 0
        self.potemkin_rating = 100
        self.power_rating = 0
        self.matches = []
        self.filtered_matches = []

    def update_stats(self,
        goals_scored,
        goals_conceded,
        result,
        is_home_game,
        opponent,
        overtime,
        match_id,
        game_data
    ):
        """
        Обновление статистики команды после матча

        Args:
            goals_scored: Забитые голы
            goals_conceded: Пропущенные голы
            result: Результат матча
            is_home_game: Является ли матч домашним
            opponent: Соперник
            overtime: Было ли продленное время
            match_id: ID матча
            game_data: Дата матча
        """
        name_sport = self.name.sports.sportName
        self.games_played += 1
        self.goals_scored += goals_scored
        self.goals_conceded += goals_conceded
        self.matches.append(
            (goals_scored,
             goals_conceded,
             result,
             is_home_game,
             opponent,
             match_id,
             game_data)
        )
        # Безопасная проверка overtime - обрабатываем NA значения
        is_overtime_ot_or_ap = not pd.isna(overtime) and (overtime == 'ot' or overtime == 'ap')
        is_overtime_empty = pd.isna(overtime) or overtime == ''
        
        if result == 'win':
            if is_overtime_ot_or_ap:
                self.overtime_wins += 1
            if goals_conceded == 0:
                self.victory_dry += 1
            self.games_wins += 1
            self.points += 3 if is_overtime_empty else 2

        elif result == 'draw':
            self.games_draws += 1
        elif result == 'loss':
            if is_overtime_ot_or_ap:
                self.overtime_losses += 1
            self.games_losses += 1
            if goals_scored == 0:
                self.lossing_dry += 1

        if goals_scored + goals_conceded >= SIZE_TOTAL[name_sport]:
            self.tb_points += 1
        else:
            self.tm_points += 1

        if goals_scored >= SIZE_ITOTAL[name_sport]:
            self.itb_points += 1
        else:
            self.itm_points += 1

        if goals_scored > 0 and goals_conceded > 0:
            self.oz_points += 1
        else:
            self.ozn_points += 1

        for attr in dir(self):
            if attr.startswith("tb"):
                try:
                    total = float(attr[2:4])/10
                    if (
                        self.goals_scored + self.goals_conceded
                            >= total
                    ):
                        setattr(self, attr, getattr(self, attr) + 1)
                except ValueError:
                    pass
            if attr.startswith("tm"):
                try:
                    total = float(attr[2:4])/10
                    if (
                        self.goals_scored + self.goals_conceded
                            < total
                    ):
                        setattr(self, attr, getattr(self, attr) + 1)
                except ValueError:
                    pass
            if attr.startswith("itb"):
                try:
                    total = float(attr[2:4])/10
                    if self.goals_scored >= total:
                        setattr(self, attr, getattr(self, attr) + 1)
                except ValueError:
                    pass
            if attr.startswith("itm"):
                try:
                    total = float(attr[2:4])/10
                    if self.goals_conceded < total:
                        setattr(self, attr, getattr(self, attr) + 1)
                except ValueError:
                    pass
    @property
    def get_goal_difference(self):
        """Разница голов"""
        return self.goals_scored - self.goals_conceded

    @property
    def get_goal_amount(self):
        """Общее количество голов"""
        return self.goals_scored + self.goals_conceded

    @property
    def get_goal_ratio(self):
        """Соотношение голов"""
        return (
            self.goals_scored / self.goals_conceded
                if self.goals_conceded != 0 else self.goals_scored
        )

    @property
    def get_average_scoring(self):
        """Средняя результативность"""
        return (
            self.goals_scored / self.games_played
                if self.games_played != 0 else 0
        )

    @property
    def get_average_throughput(self):
        """Средняя пропускаемость"""
        return (
            self.goals_conceded / self.games_played
                if self.games_played != 0 else 0
        )

    def elo_update(
            self,
            opponent_elo,
            actual_score,
            is_home_game,
            opponent_strength
    ):
        """
        Обновление ЭЛО-рейтинга с учетом результата матча,
        домашней/гостевой игры и силы соперника.

        Args:
            opponent_elo: ЭЛО рейтинг соперника
            actual_score: Фактический результат
                (1 - победа, 0.5 - ничья, 0 - поражение)
            is_home_game: True - если команда играет дома,
            False - команда играет на выезде
            opponent_strength: Сила соперника
                (1 - сильный, 0.5 - средний, 0 - слабый)
        """
        # Преимущество для домашних матчей
        home_advantage = 1.1 if is_home_game else 1.0
        expected_score = 1 / (
                1 + 10 ** ((opponent_elo - self.elo_rating) / 400)
        )

        if self.elo_rating >= 2400:
            k = 10
        elif 2400 > self.elo_rating >= 1500:
            k = 15
        else:
            k = 25

        # Корректируем "k" в зависимости от силы соперника
        if opponent_strength == 1:
            # Сильный соперник
            k = k * 1.2
        elif opponent_strength == 0:
            # Слабый соперник
            k = k * 0.8

        # Обновляем рейтинг с учетом преимущества своего поля
        self.elo_rating = (self.elo_rating + k * home_advantage *
                           (actual_score - expected_score))


class Match:
    """
    Класс матча, который обновляет статистику команд и рейтинги.

    Attributes:
        match_id: ID матча
        sport_id: ID вида спорта
        country_id: ID страны
        tournament_id: ID турнира
        game_data: Дата матча
        home_team: Домашняя команда
        away_team: Гостевая команда
        home_goals: Голы домашней команды
        away_goals: Голы гостевой команды
        overtime: Информация о дополнительном времени
        season_id: ID сезона
        stages_id: ID этапа
    """
    def __init__(self,
        match_id: int,
        sport_id: int,
        country_id: int,
        tournament_id: int,
        game_data: datetime,
        home_team: Team,
        away_team: Team,
        home_goals: int,
        away_goals: int,
        overtime: str,
        season_id: int,
        stages_id: int
    ):
        self.match_id = match_id
        self.sport_id = sport_id
        self.country_id = country_id
        self.tournament_id = tournament_id
        self.game_data = game_data
        self.home_team = home_team
        self.away_team = away_team
        self.home_goals = home_goals
        self.away_goals = away_goals
        self.overtime = overtime
        self.season_id = season_id
        self.stages_id = stages_id

    def play(self):
        """Обработка матча и обновление статистики команд"""
        # Безопасная проверка overtime - обрабатываем NA значения
        is_overtime_empty = pd.isna(self.overtime) or self.overtime == ''
        
        if self.home_goals > self.away_goals and is_overtime_empty:
            home_result = 'win'
            away_result = 'loss'
        elif self.home_goals < self.away_goals and is_overtime_empty:
            home_result = 'loss'
            away_result = 'win'
        else:
            home_result = 'draw'
            away_result = 'draw'
        self.home_team.update_stats(
            self.home_goals,
            self.away_goals,
            home_result,
            is_home_game=True,
            opponent=self.away_team,
            overtime=self.overtime,
            match_id=self.match_id,
            game_data=self.game_data
        )
        self.away_team.update_stats(
            self.away_goals,
            self.home_goals,
            away_result,
            is_home_game=False,
            opponent=self.home_team,
            overtime=self.overtime,
            match_id=self.match_id,
            game_data=self.game_data
        )


class TableStrategy(ABC):
    """
    Абстрактный класс для стратегий фильтрации матчей и расчета таблиц
    """
    def filter_matches(self, teams):
        """
        Фильтрация матчей для расчета статистики

        Args:
            teams: Словарь команд
        """
        raise NotImplementedError(
            'Метод filter_matches должен быть переопределен.'
        )

    @staticmethod
    def calculate_ratings():
        """
        Расчет рейтингов для всех стратегий
        """
        rating_strategies = [
            # DIFRatingStrategy(),
            # VORatingStrategy(),
            # ELORatingStrategy(),
            # PotemkinRatingStrategy(),
            # PowerRatingStrategy()
        ]
        tournament = Tournament()
        for strategy in rating_strategies:
            strategy.calculate_ratings(
                [
                    team for team in tournament.teams.values()
                        if team.filtered_matches
                ]
            )

    @staticmethod
    def get_standings():
        """
        Получение турнирной таблицы

        Returns:
            dict: Словарь с данными турнирной таблицы
        """
        tournament = Tournament()
        standings = {}
        for team in tournament.teams.values():
            matches = team.filtered_matches
            if len(matches) > 0:
                match_id = matches[0][5]
                game_data = matches[0][6]
                pass
            else:
                continue
            sport_name = team.name.sports.sportName
            goals_scored = sum(match[0] for match in matches)
            goals_conceded = sum(match[1] for match in matches)
            goal_difference = goals_scored - goals_conceded
            goal_amount = goals_scored + goals_conceded
            games_wins = sum(
                1 for t in matches for s in t if s == 'win'
            )
            games_draws = sum(
                1 for t in matches for s in t if s == 'draw'
            )
            games_losses = sum(
                1 for t in matches for s in t if s == 'loss'
            )
            goal_ratio = round(
                goals_scored / goals_conceded
                    if goals_conceded != 0 else goals_scored, 2
            )
            points = sum(
                3 if match[2] == 'win' else 1
                    if match[2] == 'draw' else 0 for match in matches
            )
            tb_points = len([
                t for t in matches
                    if t[0] + t[1] >= SIZE_TOTAL[sport_name]
            ])
            tb05_points = len(
                [t for t in matches if t[0] + t[1] >= 0.5]
            )
            tb15_points = len(
                [t for t in matches if t[0] + t[1] >= 1.5]
            )
            tb25_points = len(
                [t for t in matches if t[0] + t[1] >= 2.5]
            )
            tb35_points = len(
                [t for t in matches if t[0] + t[1] >= 3.5]
            )
            tb45_points = len(
                [t for t in matches if t[0] + t[1] >= 4.5]
            )
            tb55_points = len(
                [t for t in matches if t[0] + t[1] >= 5.5]
            )
            tm_points = len([
                t for t in matches
                    if t[0] + t[1] < SIZE_TOTAL[sport_name]
            ])
            tm05_points = len(
                [t for t in matches if t[0] + t[1] < 0.5]
            )
            tm15_points = len(
                [t for t in matches if t[0] + t[1] < 1.5]
            )
            tm25_points = len(
                [t for t in matches if t[0] + t[1] < 2.5]
            )
            tm35_points = len(
                [t for t in matches if t[0] + t[1] < 3.5]
            )
            tm45_points = len(
                [t for t in matches if t[0] + t[1] < 4.5]
            )
            tm55_points = len(
                [t for t in matches if t[0] + t[1] < 5.5]
            )
            itb_points = len([
                t for t in matches if t[0] >= SIZE_ITOTAL[sport_name]
            ])
            itb05_points = len([t for t in matches if t[0] >= 0.5])
            itb15_points = len([t for t in matches if t[0] >= 1.5])
            itb25_points = len([t for t in matches if t[0] >= 2.5])
            itb35_points = len([t for t in matches if t[0] >= 3.5])
            itb45_points = len([t for t in matches if t[0] >= 4.5])
            itb55_points = len([t for t in matches if t[0] >= 5.5])
            itm_points = len([
                t for t in matches if t[0] < SIZE_ITOTAL[sport_name]
            ])
            itm05_points = len([t for t in matches if t[0] < 0.5])
            itm15_points = len([t for t in matches if t[0] < 1.5])
            itm25_points = len([t for t in matches if t[0] < 2.5])
            itm35_points = len([t for t in matches if t[0] < 3.5])
            itm45_points = len([t for t in matches if t[0] < 4.5])
            itm55_points = len([t for t in matches if t[0] < 5.5])
            oz_points = len([
                t for t in matches if t[0] > 0 and t[1] > 0
            ])
            ozn_points = len([
                t for t in matches if t[0] == 0 or t[1] == 0
            ])
            average_scoring = round(
                goals_scored / team.games_played
                    if team.games_played != 0 else 0, 2
            )
            average_throughput = round(
                goals_conceded / team.games_played
                    if team.games_played != 0 else 0, 2
            )
            victory_dry = sum(
                1 for t in matches for s in t
                    if s == 'win' and t[1] == 0
            )
            lossing_dry = sum(
                1 for t in matches for s in t
                    if s == 'loss' and t[0] == 0
            )
            overtime_losses = sum(
                1 for t in matches for s in t
                    if s == 'draw' and t[0] < t[1]
            )
            overtime_wins = sum(
                1 for t in matches for s in t
                    if s == 'draw' and t[0] > t[1]
            )
            standings[team.name.id] = {
                'match_id': match_id,
                'team': team.name,
                'games_played': len(matches),
                'games_wins': games_wins,
                'games_draws': games_draws,
                'games_losses': games_losses,
                'goals_scored': goals_scored,
                'goals_conceded': goals_conceded,
                'goals_difference': goal_difference,
                'goals_amount': goal_amount,
                'goals_ratio': goal_ratio,
                'points': points,
                'average_scoring': average_scoring,
                'average_throughput': average_throughput,
                'victory_dry': victory_dry,
                'lossing_dry': lossing_dry,
                'tb_points': tb_points,
                'tb05_points': tb05_points,
                'tb15_points': tb15_points,
                'tb25_points': tb25_points,
                'tb35_points': tb35_points,
                'tb45_points': tb45_points,
                'tb55_points': tb55_points,
                'tm_points': tm_points,
                'tm05_points': tm05_points,
                'tm15_points': tm15_points,
                'tm25_points': tm25_points,
                'tm35_points': tm35_points,
                'tm45_points': tm45_points,
                'tm55_points': tm55_points,
                'itb_points': itb_points,
                'itb05_points': itb05_points,
                'itb15_points': itb15_points,
                'itb25_points': itb25_points,
                'itb35_points': itb35_points,
                'itb45_points': itb45_points,
                'itb55_points': itb55_points,
                'itm_points': itm_points,
                'itm05_points': itm05_points,
                'itm15_points': itm15_points,
                'itm25_points': itm25_points,
                'itm35_points': itm35_points,
                'itm45_points': itm45_points,
                'itm55_points': itm55_points,
                'overtime_losses': overtime_losses,
                'overtime_wins': overtime_wins,
                'oz_points': oz_points,
                'ozn_points': ozn_points,
                'dif_rating': round(team.dif_rating, 2),
                'vo_rating': round(team.vo_rating, 2),
                'elo_rating': round(team.elo_rating, 2),
                'potemkin_rating': round(team.potemkin_rating, 2),
                'power_rating': round(team.power_rating, 2),
                'gameData': game_data
            }
        # return sorted(
        #     standings, key=lambda x: (
        #         -x['points'], x['goals_difference'], -x['goals_scored']
        #     )
        # )
        return standings

    def get_standings_elo(self):
        raise NotImplementedError(
            'Метод get_standings_elo должен быть переопределен.'
        )


class GeneralTableStrategy(TableStrategy):
    """Стратегия общих данных"""

    def filter_matches(self, teams):
        """
        Фильтрация матчей - выбираются все матчи

        Args:
            teams: Словарь команд
        """
        for team in teams.values():
            team.filtered_matches = team.matches


class HomeGamesTableStrategy(GeneralTableStrategy):
    """
    Класс для выполнения расчетов турнирной таблицы и рейтингов.
    Параметры отбора: игры дома, игры в гостях и
    итоговая турнирная таблица.
    """
    def filter_matches(self, teams):
        """
        Отбор только домашних матчей.
        Весь расчет будет основан на этом списке команд.

        Args:
            teams: Словарь команд
        """
        for team in teams.values():
            team.filtered_matches = [
                match for match in team.matches if match[3]
            ]


class AwayGamesTableStrategy(GeneralTableStrategy):
    """Стратегия фильтрации гостевых матчей"""

    def filter_matches(self, teams):
        """
         Отбор только гостевых матчей.

         Args:
             teams: Словарь команд
         """
        for team in teams.values():
            team.filtered_matches = [
                match for match in team.matches if not match[3]
            ]


class StrongOpponentsTableStrategy(TableStrategy):
    """Стратегия фильтрации матчей с сильными соперниками"""

    def get_strong_teams(self, teams):
        """
        Отбор команд из верхней части турнирной таблицы.

        Args:
            teams: Словарь команд

        Returns:
            list: Сильные команды
        """
        total_teams = len(teams)
        sorted_teams = sorted(teams.values(), key=lambda x: -x.points)
        strong_teams = sorted_teams[:total_teams // 3]
        return strong_teams

    def filter_matches(self, teams):
        """
        Фильтрация матчей с сильными соперниками.

        Args:
            teams: Словарь команд
        """
        strong_teams = self.get_strong_teams(teams)
        for team in teams.values():
            team.filtered_matches = [
                match for match in team.matches
                    if match[4] in strong_teams
            ]


class MediumOpponentsTableStrategy(StrongOpponentsTableStrategy):
    """Стратегия фильтрации матчей со средними соперниками"""

    def get_strong_teams(self, teams):
        """
        Отбор команд из средней части турнирной таблицы.

        Args:
            teams: Словарь команд

        Returns:
            list: Средние команды
        """
        total_teams = len(teams)
        sorted_teams = sorted(teams.values(), key=lambda x: -x.points)
        medium_teams = sorted_teams[
                       total_teams // 3:2 * total_teams // 3
                       ]
        return medium_teams


class WeakOpponentsTableStrategy(StrongOpponentsTableStrategy):
    """Стратегия фильтрации матчей со слабыми соперниками"""

    def get_strong_teams(self, teams):
        """
        Отбор команд из нижней части турнирной таблицы.

        Args:
            teams: Словарь команд

        Returns:
            list: Слабые команды
        """
        total_teams = len(teams)
        sorted_teams = sorted(teams.values(), key=lambda x: -x.points)
        weak_teams = sorted_teams[2 * total_teams // 3:]
        return weak_teams


class HomeGamesStrongOpponentsTableStrategy(
    GeneralTableStrategy,
    StrongOpponentsTableStrategy
):
    """Стратегия фильтрации домашних матчей с сильными соперниками"""

    def get_strong_teams(self, teams):
        """
        Отбор команд из верхней части турнирной таблицы.

        Args:
            teams: Словарь команд

        Returns:
            list: Сильные команды
        """
        total_teams = len(teams)
        sorted_teams = sorted(teams.values(), key=lambda x: -x.points)
        strong_teams = sorted_teams[:total_teams // 3]
        return strong_teams

    def filter_matches(self, teams):
        """
        Фильтрация домашних матчей с сильными соперниками.

        Args:
            teams: Словарь команд
        """
        strong_teams = self.get_strong_teams(teams)
        for team in teams.values():
            team.filtered_matches = [
                match for match in team.matches
                    if match[4] in strong_teams and match[3]
            ]


class HomeGamesMediumOpponentsTableStrategy(
    GeneralTableStrategy,
    StrongOpponentsTableStrategy
):
    """Стратегия фильтрации домашних матчей со средними соперниками"""

    def get_strong_teams(self, teams):
        """
        Отбор команд из средней части турнирной таблицы.

        Args:
            teams: Словарь команд

        Returns:
            list: Средние команды
        """
        total_teams = len(teams)
        sorted_teams = sorted(teams.values(), key=lambda x: -x.points)
        medium_teams = sorted_teams[
                       total_teams // 3:2 * total_teams // 3
                       ]
        return medium_teams

    def filter_matches(self, teams):
        """
        Фильтрация домашних матчей со средними соперниками.

        Args:
            teams: Словарь команд
        """
        medium_teams = self.get_strong_teams(teams)
        for team in teams.values():
            team.filtered_matches = [
                match for match in team.matches
                    if match[4] in medium_teams and match[3]
            ]


class HomeGamesWeakOpponentsTableStrategy(
    GeneralTableStrategy,
    StrongOpponentsTableStrategy
):
    """Стратегия фильтрации домашних матчей со слабыми соперниками"""

    def get_strong_teams(self, teams):
        """
        Отбор команд из нижней части турнирной таблицы.

        Args:
            teams: Словарь команд

        Returns:
            list: Слабые команды
        """
        total_teams = len(teams)
        sorted_teams = sorted(teams.values(), key=lambda x: -x.points)
        weak_teams = sorted_teams[2 * total_teams // 3:]
        return weak_teams

    def filter_matches(self, teams):
        """
        Фильтрация домашних матчей со слабыми соперниками.

        Args:
            teams: Словарь команд
        """
        weak_teams = self.get_strong_teams(teams)
        for team in teams.values():
            team.filtered_matches = [
                match for match in team.matches
                    if match[4] in weak_teams and match[3]
            ]


class AwayGamesStrongOpponentsTableStrategy(
    GeneralTableStrategy,
    StrongOpponentsTableStrategy
):
    """Стратегия фильтрации гостевых матчей с сильными соперниками"""

    def get_strong_teams(self, teams):
        """
        Отбор команд из верхней части турнирной таблицы.

        Args:
            teams: Словарь команд

        Returns:
            list: Сильные команды
        """
        total_teams = len(teams)
        sorted_teams = sorted(teams.values(), key=lambda x: -x.points)
        strong_teams = sorted_teams[:total_teams // 3]
        return strong_teams

    def filter_matches(self, teams):
        """
        Фильтрация гостевых матчей с сильными соперниками.

        Args:
            teams: Словарь команд
        """
        strong_teams = self.get_strong_teams(teams)
        for team in teams.values():
            team.filtered_matches = [
                match for match in team.matches
                    if match[4] in strong_teams and not match[3]
            ]


class AwayGamesMediumOpponentsTableStrategy(
    GeneralTableStrategy,
    StrongOpponentsTableStrategy
):
    """Стратегия фильтрации гостевых матчей со средними соперниками"""

    def get_strong_teams(self, teams):
        """
        Отбор команд из средней части турнирной таблицы.

        Args:
            teams: Словарь команд

        Returns:
            list: Средние команды
        """
        total_teams = len(teams)
        sorted_teams = sorted(teams.values(), key=lambda x: -x.points)
        medium_teams = sorted_teams[
                       total_teams // 3:2 * total_teams // 3
                       ]
        return medium_teams

    def filter_matches(self, teams):
        """
        Фильтрация гостевых матчей со средними соперниками.

        Args:
            teams: Словарь команд
        """
        medium_teams = self.get_strong_teams(teams)
        for team in teams.values():
            team.filtered_matches = [
                match for match in team.matches
                    if match[4] in medium_teams and not match[3]
            ]


class AwayGamesWeakOpponentsTableStrategy(
    GeneralTableStrategy,
    StrongOpponentsTableStrategy
):
    """Стратегия фильтрации гостевых матчей со слабыми соперниками"""

    def get_strong_teams(self, teams):
        """
        Отбор команд из нижней части турнирной таблицы.

        Args:
            teams: Словарь команд

        Returns:
            list: Слабые команды
        """
        total_teams = len(teams)
        sorted_teams = sorted(teams.values(), key=lambda x: -x.points)
        weak_teams = sorted_teams[2 * total_teams // 3:]
        return weak_teams

    def filter_matches(self, teams):
        """
        Фильтрация гостевых матчей со слабыми соперниками.

        Args:
            teams: Словарь команд
        """
        weak_teams = self.get_strong_teams(teams)
        for team in teams.values():
            team.filtered_matches = [
                match for match in team.matches
                    if match[4] in weak_teams and not match[3]
            ]