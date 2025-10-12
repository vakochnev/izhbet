# izhbet/core/feature_engineer.py
"""
Модуль для feature engineering.
"""

import logging
import numpy as np
import pandas as pd
from typing import List, Dict, Optional, Tuple
from sklearn.feature_selection import SelectKBest, f_classif, mutual_info_classif
from sklearn.decomposition import PCA
from sklearn.preprocessing import PolynomialFeatures
from sklearn.cluster import KMeans

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Класс для feature engineering."""

    def __init__(self, random_state: int = 42):
        self.random_state = random_state
        self.feature_importances_ = None

    def calculate_feature_importance(self, X: np.ndarray, y: np.ndarray) -> Dict:
        """
        Расчет важности признаков.
        """
        # ANOVA F-value
        selector_anova = SelectKBest(score_func=f_classif, k='all')
        selector_anova.fit(X, y)
        anova_scores = selector_anova.scores_

        # Mutual Information
        selector_mi = SelectKBest(score_func=mutual_info_classif, k='all')
        selector_mi.fit(X, y)
        mi_scores = selector_mi.scores_

        # Комбинированная важность
        combined_scores = (anova_scores + mi_scores) / 2

        importance_df = pd.DataFrame({
            'feature': range(X.shape[1]),
            'anova_score': anova_scores,
            'mi_score': mi_scores,
            'combined_score': combined_scores
        }).sort_values('combined_score', ascending=False)

        self.feature_importances_ = importance_df
        return importance_df.to_dict('records')

    def select_top_features(self, X: np.ndarray, n_features: int = 50) -> np.ndarray:
        """
        Выбор топ-N наиболее важных признаков.
        """
        if self.feature_importances_ is None:
            raise ValueError("Сначала вызовите calculate_feature_importance")

        top_features = self.feature_importances_.head(n_features)['feature'].values
        return X[:, top_features]

    def create_polynomial_features(self, X: np.ndarray, degree: int = 2) -> np.ndarray:
        """
        Создание полиномиальных features.
        """
        poly = PolynomialFeatures(degree=degree, include_bias=False)
        X_poly = poly.fit_transform(X)

        logger.info(f"Полиномиальные features: {X_poly.shape[1]} признаков")
        return X_poly

    def create_interaction_features(self, X: np.ndarray) -> np.ndarray:
        """
        Создание interaction features.
        """
        n_samples, n_features = X.shape
        interaction_features = []

        for i in range(n_features):
            for j in range(i + 1, n_features):
                interaction = X[:, i] * X[:, j]
                interaction_features.append(interaction)

        X_interaction = np.column_stack([X] + interaction_features)
        logger.info(f"Interaction features: {X_interaction.shape[1]} признаков")
        return X_interaction

    def create_cluster_features(self, X: np.ndarray, n_clusters: int = 5) -> np.ndarray:
        """
        Создание cluster-based features.
        """
        kmeans = KMeans(n_clusters=n_clusters, random_state=self.random_state)
        clusters = kmeans.fit_predict(X)

        # One-hot encoding кластеров
        cluster_features = np.eye(n_clusters)[clusters]

        # Расстояния до центроидов
        distances = kmeans.transform(X)

        X_with_clusters = np.column_stack([X, cluster_features, distances])
        logger.info(f"Cluster features: {X_with_clusters.shape[1]} признаков")
        return X_with_clusters

    def apply_pca(self, X: np.ndarray, n_components: Optional[int] = None,
                 variance_threshold: float = 0.95) -> np.ndarray:
        """
        Применение PCA для reduction dimensionality.
        """
        if n_components is None:
            # Автоматический выбор компонент по explained variance
            pca = PCA(n_components=variance_threshold, random_state=self.random_state)
            X_pca = pca.fit_transform(X)
            logger.info(f"PCA: {X_pca.shape[1]} компонент объясняют {variance_threshold*100}% variance")
        else:
            pca = PCA(n_components=n_components, random_state=self.random_state)
            X_pca = pca.fit_transform(X)
            logger.info(f"PCA: {X_pca.shape[1]} компонент")

        return X_pca

    def create_ratio_features(self, X: np.ndarray, feature_names: Optional[List[str]] = None) -> np.ndarray:
        """
        Создание ratio features (отношения между признаками).
        """
        ratios = []
        ratio_names = []

        n_features = X.shape[1]

        for i in range(n_features):
            for j in range(i + 1, n_features):
                # Избегаем деления на ноль
                with np.errstate(divide='ignore', invalid='ignore'):
                    ratio = np.divide(X[:, i], X[:, j])
                    ratio[~np.isfinite(ratio)] = 0  # Заменяем inf/nan на 0

                ratios.append(ratio)

                if feature_names:
                    ratio_names.append(f"{feature_names[i]}_{feature_names[j]}_ratio")

        X_with_ratios = np.column_stack([X] + ratios)
        logger.info(f"Ratio features: {X_with_ratios.shape[1]} признаков")
        return X_with_ratios

    def engineer_features(
        self,
        X: np.ndarray,
        y: np.ndarray,
        steps: List[str] = ['importance', 'poly', 'interaction', 'pca'],
        **kwargs
    ) -> np.ndarray:
        """
        Полный pipeline feature engineering.
        """
        X_engineered = X.copy()

        if 'importance' in steps:
            self.calculate_feature_importance(X, y)
            n_features = kwargs.get('n_top_features', 50)
            X_engineered = self.select_top_features(X_engineered, n_features)

        if 'poly' in steps:
            degree = kwargs.get('poly_degree', 2)
            X_engineered = self.create_polynomial_features(X_engineered, degree)

        if 'interaction' in steps:
            X_engineered = self.create_interaction_features(X_engineered)

        if 'clusters' in steps:
            n_clusters = kwargs.get('n_clusters', 5)
            X_engineered = self.create_cluster_features(X_engineered, n_clusters)

        if 'ratios' in steps:
            feature_names = kwargs.get('feature_names')
            X_engineered = self.create_ratio_features(X_engineered, feature_names)

        if 'pca' in steps:
            n_components = kwargs.get('pca_components')
            variance_threshold = kwargs.get('variance_threshold', 0.95)
            X_engineered = self.apply_pca(X_engineered, n_components, variance_threshold)

        return X_engineered