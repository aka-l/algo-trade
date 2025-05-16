import pandas as pd
from datetime import datetime, timedelta
import os
import pytz

# Добавим отладочную информацию
print("Текущая директория:", os.getcwd())
print("Проверяем существование файла:", os.path.exists('/home/aka/Documents/algoTrading/Whale_bots_transactions/.csv/whale_alert_transactions.csv'))

# Проверяем существование файла
if not os.path.exists('/home/aka/Documents/algoTrading/Whale_bots_transactions/.csv/whale_alert_transactions.csv'):
    raise FileNotFoundError(f"Файл не найден по пути: /home/aka/Documents/algoTrading/Whale_bots_transactions/cvx/whale_alert_transactions.csv")

# Загружаем CSV файл
df = pd.read_csv('/home/aka/Documents/algoTrading/Whale_bots_transactions/.csv/whale_alert_transactions.csv')

# Преобразуем даты
df['date'] = pd.to_datetime(df['date'])

# Получаем дату 30 дней назад
date_30_days_ago = datetime.now(pytz.UTC) - timedelta(days=30)

# Фильтруем данные по дате и значениям BTC (547, 840, 960)
filtered_df = df[
    (df['date'] >= date_30_days_ago) & 
    (df['btc'].isin([547.0, 840.0, 960.0]))
]

# Выводим результат
print("\nОтфильтрованные транзакции:")
print(filtered_df)

# Статистика
print("\nКоличество найденных транзакций:", len(filtered_df))
print("Общий объем BTC:", filtered_df['btc'].sum())

# Если нужно сохранить результат в новый CSV файл:
# filtered_df.to_csv('filtered_transactions.csv', index=False)
