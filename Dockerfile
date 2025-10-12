# Используем официальный образ Python
FROM python:3.12-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем файл с зависимостями
COPY requirements.txt .

# Устанавливаем зависимости
RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt --no-cache-dir

# Копируем все файлы приложения в контейнер
COPY . .

# Указываем команду для запуска приложения
CMD ["./run.sh"]
