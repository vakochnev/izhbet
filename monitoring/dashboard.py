# izhbet/monitoring/dashboard.py
"""
Дашборд для визуализации мониторинга моделей с поддержкой чемпионатов.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import List, Dict, Any

from db.queries.metrics import (
    get_available_championships,
    get_metrics_by_championship,
    get_models_by_championship,
    get_metrics_history,
    get_championship_stats
)


class ChampionshipMonitoringDashboard:
    """Дашборд для мониторинга моделей по чемпионатам."""

    def run_dashboard(self):
        """Запуск дашборда."""
        st.set_page_config(
            page_title="Мониторинг моделей по чемпионатам",
            layout="wide"
        )
        st.title("🏆 Мониторинг качества моделей по чемпионатам")

        # Загрузка данных о чемпионатах
        championships = self._get_available_championships()

        if not championships:
            st.warning("Нет данных о чемпионатах. Запустите мониторинг сначала.")
            return

        # Сайдбар с выбором чемпионата
        st.sidebar.header("Настройки просмотра")

        # Выбор режима просмотра
        view_mode = st.sidebar.radio(
            "Режим просмотра",
            ["Обзор чемпионатов", "Детализация чемпионата", "Сравнение чемпионатов"]
        )

        # Выбор периода
        days = st.sidebar.slider("Период анализа (дни)", 1, 90, 30)

        if view_mode == "Обзор чемпионатов":
            self.show_championships_overview(championships, days)
        elif view_mode == "Детализация чемпионата":
            self.show_championship_details(championships, days)
        else:
            self.show_championships_comparison(championships, days)

    def _get_available_championships(self) -> List[Dict]:
        """Получение списка доступных чемпионатов."""
        try:
            championships = get_available_championships()
            return sorted(championships, key=lambda x: x['name'])
        except Exception as e:
            st.error(f"Ошибка загрузки чемпионатов: {e}")
            return []

    def show_championships_overview(self, championships: List[Dict], days: int):
        """Обзорная панель всех чемпионатов."""
        st.header("📊 Обзор качества моделей по чемпионатам")

        # Сбор сводной статистики
        summary_data = []

        for championship in championships:
            try:
                champ_stats = get_championship_stats(championship['id'])
                models = get_models_by_championship(championship['id'])
                summary_data.append({
                    'Чемпионат': championship['name'],
                    'ID': championship['id'],
                    'Моделей': len(models),
                    'Средняя точность': champ_stats['avg_accuracy'],
                    'Всего предсказаний': champ_stats['total_predictions'],
                    'Лучшая модель': champ_stats['best_model'],
                    'Худшая модель': champ_stats['worst_model']
                })
            except Exception as e:
                st.warning(f"Ошибка загрузки данных для чемпионата {championship['name']}: {e}")
                continue

        if not summary_data:
            st.info("Нет данных для отображения")
            return

        df_summary = pd.DataFrame(summary_data)

        # Вкладки с разной информацией
        tab1, tab2, tab3 = st.tabs(["Сводная таблица", "Визуализация", "Топ чемпионаты"])

        with tab1:
            st.dataframe(
                df_summary.style.format({
                    'Средняя точность': '{:.2%}',
                    'Всего предсказаний': '{:,}'
                }).background_gradient(subset=['Средняя точность'], cmap='RdYlGn'),
                use_container_width=True
            )

        with tab2:
            # Heatmap точности по чемпионатам
            fig = px.imshow(
                df_summary.pivot_table(values='Средняя точность', index='Чемпионат', aggfunc='mean'),
                title='Точность моделей по чемпионатам',
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig, use_container_width=True)

            # Scatter plot: точность vs количество предсказаний
            fig = px.scatter(
                df_summary,
                x='Всего предсказаний',
                y='Средняя точность',
                size='Моделей',
                color='Чемпионат',
                hover_name='Чемпионат',
                title='Точность vs Объем данных по чемпионатам'
            )
            st.plotly_chart(fig, use_container_width=True)

        with tab3:
            # Топ-5 чемпионатов по точности
            top_championships = df_summary.nlargest(5, 'Средняя точность')
            fig = px.bar(
                top_championships,
                x='Чемпионат',
                y='Средняя точность',
                title='Топ-5 чемпионатов по точности прогнозов',
                color='Средняя точность',
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig, use_container_width=True)

    def show_championship_details(self, championships: List[Dict], days: int):
        """Детализация по конкретному чемпионату."""
        st.header("🔍 Детализация чемпионата")

        # Выбор чемпионата
        championship_options = {champ['name']: champ for champ in championships}
        selected_champ_name = st.selectbox(
            "Выберите чемпионат",
            list(championship_options.keys())
        )

        selected_champ = championship_options[selected_champ_name]

        # Загрузка метрик чемпионата
        try:
            models = get_models_by_championship(selected_champ['id'])
            if not models:
                st.warning(f"Нет данных мониторинга для чемпионата {selected_champ_name}")
                return
        except Exception as e:
            st.error(f"Ошибка загрузки данных чемпионата: {e}")
            return

        # Вкладки с детальной информацией
        tab1, tab2, tab3, tab4 = st.tabs(["Обзор", "Модели", "Тренды", "История"])

        with tab1:
            self._show_championship_overview(selected_champ, models)

        with tab2:
            self._show_models_details(models, selected_champ)

        with tab3:
            self._show_trends_tab(models, selected_champ, days)

        with tab4:
            self._show_history_tab(selected_champ['id'])

    def _show_championship_overview(self, championship: Dict, models: List[Dict]):
        """Обзорная информация по чемпионату."""
        col1, col2, col3 = st.columns(3)

        stats = get_championship_stats(championship['id'])

        with col1:
            st.metric("Всего моделей", stats['total_models'])
            st.metric("Всего предсказаний", f"{stats['total_predictions']:,}")

        with col2:
            st.metric("Средняя точность", f"{stats['avg_accuracy']:.2%}")
            st.metric("Лучшая модель", stats['best_model'])

        with col3:
            st.metric("Худшая модель", stats['worst_model'])
            st.metric("Стабильность", stats['stability'])

        # График распределения точности по моделям
        model_accuracies = []
        for model in models:
            # Для классификации используем accuracy
            if model.get('accuracy') and model['accuracy'] > 0:
                model_accuracies.append({
                    'Модель': model['model_name'], 
                    'Точность': model['accuracy']
                })
            # Для регрессии используем обратное значение MAE (чем меньше MAE, тем лучше)
            elif model.get('mae') and model['mae'] > 0:
                # Адаптивная нормализация MAE в зависимости от типа модели
                mae_value = model['mae']
                model_name = model['model_name']
                
                # Разные пороги нормализации для разных типов моделей
                if 'total_amount' in model_name:
                    # Для моделей общего тотала используем более высокий порог
                    max_mae = 5.0
                elif 'total_home' in model_name or 'total_away' in model_name:
                    # Для индивидуальных тоталов используем средний порог
                    max_mae = 3.0
                else:
                    # Для остальных моделей используем стандартный порог
                    max_mae = 2.0
                
                # Нормализуем MAE к [0,1] с адаптивным порогом
                normalized_mae = min(mae_value / max_mae, 1.0)
                accuracy_score = 1 - normalized_mae
                
                model_accuracies.append({
                    'Модель': f"{model['model_name']} (MAE: {mae_value:.2f})",
                    'Точность': accuracy_score
                })

        if model_accuracies:
            df_acc = pd.DataFrame(model_accuracies)
            fig = px.bar(df_acc, x='Модель', y='Точность',
                         title='Точность моделей в чемпионате')
            st.plotly_chart(fig, use_container_width=True)

    def _show_models_details(self, models: List[Dict], championship: Dict):
        """Детализация по моделям чемпионата."""
        st.subheader("Детализация по моделям")

        model_names = [model['model_name'] for model in models]
        selected_model_name = st.selectbox("Выберите модель", model_names)

        if selected_model_name:
            selected_model = next((m for m in models if m['model_name'] == selected_model_name), None)
            
            if selected_model:
                col1, col2 = st.columns(2)

                with col1:
                    st.write("**Текущие метрики:**")
                    # Метрики для классификации
                    if selected_model.get('accuracy') is not None:
                        st.metric("Accuracy", f"{selected_model['accuracy']:.2%}")
                    if selected_model.get('precision') is not None:
                        st.metric("Precision", f"{selected_model['precision']:.3f}")
                    if selected_model.get('recall') is not None:
                        st.metric("Recall", f"{selected_model['recall']:.3f}")
                    if selected_model.get('f1_score') is not None:
                        st.metric("F1 Score", f"{selected_model['f1_score']:.3f}")
                    
                    # Метрики для регрессии
                    if selected_model.get('mae') is not None:
                        st.metric("MAE", f"{selected_model['mae']:.3f}")
                    if selected_model.get('mse') is not None:
                        st.metric("MSE", f"{selected_model['mse']:.3f}")
                    if selected_model.get('rmse') is not None:
                        st.metric("RMSE", f"{selected_model['rmse']:.3f}")
                    if selected_model.get('min_error') is not None:
                        st.metric("Min Error", f"{selected_model['min_error']:.3f}")
                    if selected_model.get('max_error') is not None:
                        st.metric("Max Error", f"{selected_model['max_error']:.3f}")

                with col2:
                    st.write("**Информация о модели:**")
                    st.write(f"**Тип модели:** {selected_model.get('model_type', 'N/A')}")
                    st.write(f"**Дата обучения:** {selected_model.get('training_date', 'N/A')}")
                    st.write(f"**Название:** {selected_model.get('model_name', 'N/A')}")

    def _show_trends_tab(self, models: List[Dict], championship: Dict, days: int):
        """Вкладка с трендами моделей чемпионата."""
        st.subheader("Тренды метрик")

        # Мультиселект моделей для сравнения
        model_names = [model['model_name'] for model in models]
        selected_models = st.multiselect(
            "Выберите модели для сравнения",
            model_names,
            default=model_names[:2] if len(model_names) >= 2 else []
        )

        if not selected_models:
            return

        fig = go.Figure()

        for model_name in selected_models:
            try:
                # Получаем историю метрик для модели
                history = get_metrics_history(championship['id'], model_name, days)
                
                if not history:
                    continue

                # Извлекаем метрики точности для классификации
                timestamps = []
                accuracies = []
                mae_values = []

                for record in history:
                    timestamp = record['training_date']
                    timestamps.append(timestamp)
                    
                    # Для классификации используем accuracy
                    accuracy = record.get('accuracy', 0)
                    if accuracy and accuracy > 0:
                        accuracies.append(accuracy)
                    else:
                        accuracies.append(None)
                    
                    # Для регрессии используем MAE
                    mae = record.get('mae', 0)
                    if mae and mae > 0:
                        mae_values.append(mae)
                    else:
                        mae_values.append(None)

                if timestamps:
                    # Добавляем линию точности (для классификации)
                    if any(acc is not None for acc in accuracies):
                        fig.add_trace(go.Scatter(
                            x=timestamps,
                            y=accuracies,
                            name=f"{model_name} (Accuracy)",
                            mode='lines+markers',
                            line=dict(dash='solid'),
                            yaxis='y'
                        ))
                    
                    # Добавляем линию MAE (для регрессии)
                    if any(mae is not None for mae in mae_values):
                        fig.add_trace(go.Scatter(
                            x=timestamps,
                            y=mae_values,
                            name=f"{model_name} (MAE)",
                            mode='lines+markers',
                            line=dict(dash='dash'),
                            yaxis='y2'
                        ))
            except Exception as e:
                st.warning(f"Ошибка загрузки истории для модели {model_name}: {e}")
                continue

        # Настройка осей для двойного графика
        fig.update_layout(
            title=f"Тренды метрик моделей в {championship['name']}",
            xaxis_title="Дата",
            hovermode='x unified',
            yaxis=dict(
                title="Точность (Accuracy)",
                side="left",
                range=[0, 1]
            ),
            yaxis2=dict(
                title="MAE (Mean Absolute Error)",
                side="right",
                overlaying="y",
                range=[0, None]
            )
        )

        st.plotly_chart(fig, use_container_width=True)

    def show_championships_comparison(self, championships: List[Dict], days: int):
        """Сравнение чемпионатов между собой."""
        st.header("📈 Сравнение чемпионатов")

        # Выбор чемпионатов для сравнения
        championship_names = [champ['name'] for champ in championships]
        selected_championships = st.multiselect(
            "Выберите чемпионаты для сравнения",
            championship_names,
            default=championship_names[:3] if len(championship_names) >= 3 else championship_names
        )

        if not selected_championships:
            return

        # Сбор данных для сравнения
        comparison_data = []

        for champ_name in selected_championships:
            champ = next((c for c in championships if c['name'] == champ_name), None)
            if not champ:
                continue

            try:
                stats = get_championship_stats(champ['id'])
                models = get_models_by_championship(champ['id'])
                comparison_data.append({
                    'Чемпионат': champ_name,
                    'Точность': stats['avg_accuracy'],
                    'Предсказания': stats['total_predictions'],
                    'Модели': len(models)
                })
            except Exception as e:
                st.warning(f"Ошибка загрузки данных для чемпионата {champ_name}: {e}")
                continue

        if not comparison_data:
            return

        df_comparison = pd.DataFrame(comparison_data)

        # Визуализация сравнения
        col1, col2 = st.columns(2)

        with col1:
            fig = px.bar(
                df_comparison,
                x='Чемпионат',
                y='Точность',
                title='Сравнение точности чемпионатов',
                color='Точность',
                color_continuous_scale='RdYlGn'
            )
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.scatter(
                df_comparison,
                x='Предсказания',
                y='Точность',
                size='Модели',
                color='Чемпионат',
                title='Точность vs Объем данных'
            )
            st.plotly_chart(fig, use_container_width=True)


    def _show_history_tab(self, championship_id: int):
        """Вкладка с историей изменений."""
        st.subheader("История изменений метрик")

        try:
            # Получаем историю метрик для чемпионата
            history = get_metrics_history(championship_id, days=90)
            
            if not history:
                st.info("Нет исторических данных для отображения")
                return

            # Создаем DataFrame для отображения
            df_history = pd.DataFrame(history)
            
            # Показываем таблицу с историей
            st.dataframe(
                df_history.style.format({
                    'accuracy': '{:.2%}',
                    'precision': '{:.3f}',
                    'recall': '{:.3f}',
                    'f1_score': '{:.3f}'
                }),
                use_container_width=True
            )
            
            # График истории точности по всем моделям
            if 'accuracy' in df_history.columns:
                fig = px.line(
                    df_history, 
                    x='training_date', 
                    y='accuracy',
                    color='model_name',
                    title='История точности моделей'
                )
                st.plotly_chart(fig, use_container_width=True)
                
        except Exception as e:
            st.error(f"Ошибка загрузки истории метрик: {e}")


# Запуск дашборда
if __name__ == '__main__':
    dashboard = ChampionshipMonitoringDashboard()
    dashboard.run_dashboard()