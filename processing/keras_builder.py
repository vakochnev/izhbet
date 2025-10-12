# izhbet/processing/keras_builder.py
from typing import Optional, Dict, Any
import logging

from keras import models, layers, optimizers, regularizers

logger = logging.getLogger(__name__)


class KerasModelBuilder:
    """Строитель моделей Keras с улучшенной архитектурой."""

    @staticmethod
    def create_advanced_model(
            input_shape: int,
            task_type: str,
            num_classes: Optional[int] = None,
            l1_reg: float = 0.001,
            l2_reg: float = 0.001,
            dropout_rate: float = 0.5,
            initial_learning_rate: float = 0.001
    ) -> models.Sequential:
        """
        Создание улучшенной модели Keras с регуляризацией.
        """
        regularizer = regularizers.l1_l2(l1=l1_reg, l2=l2_reg)

        model = models.Sequential([
            layers.Input(shape=(input_shape,)),

            # Блок 1
            layers.Dense(units=256, activation='relu',
                         kernel_regularizer=regularizer),
            layers.BatchNormalization(),
            layers.Dropout(dropout_rate),

            # Блок 2
            layers.Dense(units=128, activation='relu',
                         kernel_regularizer=regularizer),
            layers.BatchNormalization(),
            layers.Dropout(dropout_rate * 0.9),

            # Блок 3
            layers.Dense(units=64, activation='relu',
                         kernel_regularizer=regularizer),
            layers.BatchNormalization(),
            layers.Dropout(dropout_rate * 0.8),

            # Блок 4
            layers.Dense(units=32, activation='relu',
                         kernel_regularizer=regularizer),
            layers.BatchNormalization(),
            layers.Dropout(dropout_rate * 0.7),

            # Блок 5
            layers.Dense(units=16, activation='relu',
                         kernel_regularizer=regularizer),
            layers.BatchNormalization(),
            layers.Dropout(dropout_rate * 0.6),
        ])

        # Выходной слой
        if task_type == 'classification':
            if num_classes == 1:
                # Бинарная классификация (One-Hot encoded)
                model.add(layers.Dense(1, activation='sigmoid'))
                model.compile(
                    optimizer=optimizers.Adam(learning_rate=initial_learning_rate),
                    loss='binary_crossentropy',
                    metrics=['accuracy']
                )
            elif num_classes == 2:
                # Бинарная классификация (обычная)
                model.add(layers.Dense(1, activation='sigmoid'))
                model.compile(
                    optimizer=optimizers.Adam(learning_rate=initial_learning_rate),
                    loss='binary_crossentropy',
                    metrics=['accuracy']
                )
            else:
                # Многоклассовая классификация
                model.add(layers.Dense(num_classes, activation='softmax'))
                model.compile(
                    optimizer=optimizers.Adam(learning_rate=initial_learning_rate),
                    loss='sparse_categorical_crossentropy',  # Для обычных меток
                    metrics=['accuracy']
                )
        else:  # regression
            model.add(layers.Dense(1, activation='linear'))
            model.compile(
                optimizer=optimizers.Adam(learning_rate=initial_learning_rate),
                loss='mse', #'huber_loss',  # Более устойчивая к выбросам
                metrics=['mae', 'mse']
            )

        return model

    @staticmethod
    def create_callbacks(
            models_dir: str,
            model_name: str,
            patience: int = 15,
            min_delta: float = 0.001,
            initial_learning_rate: float = 0.001
    ) -> list:
        """
        Создание улучшенных callbacks с настройками.
        """
        from keras import callbacks

        # Создаем learning rate schedule для ReduceLROnPlateau
        reduce_lr = callbacks.ReduceLROnPlateau(
            monitor='val_loss',
            factor=0.5,
            patience=patience // 2,
            min_lr=1e-6,
            verbose=0
        )

        # Альтернатива: ExponentialDecay через callback (если нужно)
        # lr_schedule = callbacks.LearningRateScheduler(
        #     lambda epoch: initial_learning_rate * 0.9 ** (epoch // 10)
        # )

        return [
            callbacks.EarlyStopping(
                monitor='val_loss',
                patience=patience,
                min_delta=min_delta,
                restore_best_weights=True,
                verbose=0
            ),
            reduce_lr,
            callbacks.ModelCheckpoint(
                filepath=f'{models_dir}/{model_name}_best_model.keras',
                monitor='val_loss',
                save_best_only=True,
                save_weights_only=False,
                verbose=0
            )
        ]

    @staticmethod
    def create_model(
            input_shape: int,
            task_type: str,
            num_classes: Optional[int] = None
    ) -> models.Sequential:
        """
        Совместимость со старым кодом - использует улучшенную архитектуру.
        """
        return KerasModelBuilder.create_advanced_model(
            input_shape=input_shape,
            task_type=task_type,
            num_classes=num_classes,
            initial_learning_rate=0.001
        )