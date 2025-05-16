import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from itertools import combinations
import joblib
import os

def create_combined_dataset(movements_file: str, transactions_file: str) -> pd.DataFrame:
    """
    Создает единый датасет, где:
    - За основу берется таблица движений цены
    - Добавляется колонка со списком всех транзакций за каждый день
    """
    # Загрузка данных
    movements_df = pd.read_csv(movements_file)
    transactions_df = pd.read_csv(transactions_file)
    
    # Удаляем колонку last_timestamp, если она есть
    if 'last_timestamp' in transactions_df.columns:
        transactions_df = transactions_df.drop('last_timestamp', axis=1)
    
    # Приведение дат к единому формату без UTC и времени
    movements_df['date'] = pd.to_datetime(movements_df['Date']).dt.tz_localize(None).dt.date
    transactions_df['date'] = pd.to_datetime(transactions_df['date']).dt.tz_localize(None).dt.date
    
    # Удаляем старую колонку Date
    movements_df = movements_df.drop('Date', axis=1)
    
    # Группировка транзакций по дням в виде списка
    daily_transactions = transactions_df.groupby('date', as_index=False).agg(
        transactions=('btc', lambda x: x.tolist())
    )
    
    # Объединение с основной таблицей движений
    result_df = pd.merge(
        movements_df,
        daily_transactions,
        on='date',
        how='left'
    )
    
    # Заполнение дней без транзакций пустым списком и удаление строк до первой транзакции
    result_df['transactions'] = result_df['transactions'].fillna('').apply(lambda x: [] if x == '' else x)
    first_transaction_idx = result_df[result_df['transactions'].str.len() > 0].index[0]
    result_df = result_df.iloc[first_transaction_idx:]
    
    return result_df

def analyze_price_movements(df: pd.DataFrame, forecast_window: int = 3) -> dict:
    """
    Анализирует связь между транзакциями и будущим движением цены используя Random Forest.
    Анализирует ВСЕ транзакции и их комбинации, отдельно для роста и падения.
    """
    # Находим колонку с процентами
    price_column = [col for col in df.columns if col.startswith('+')][0]
    
    # Создаем целевую переменную (1 - рост, 0 - падение)
    df['price_change'] = df[price_column].str.rstrip('%').astype('float')
    df['target'] = (df['price_change'] > 0).astype(int)
    
    # Подсчет частоты каждой транзакции и создание полного набора
    transaction_counts = {}
    all_transactions = set()
    for trans_list in df['transactions']:
        for t in trans_list:
            transaction_counts[t] = transaction_counts.get(t, 0) + 1
            all_transactions.add(t)
    
    print(f"\nВсего уникальных транзакций: {len(all_transactions)}")
    print("Начинаем полный анализ всех транзакций...")
    
    # Создаем базовую матрицу признаков (все одиночные транзакции)
    X_single = np.zeros((len(df), len(all_transactions)))
    transaction_map = {t: i for i, t in enumerate(all_transactions)}
    
    print("Создаем матрицу одиночных транзакций...")
    for i, trans_list in enumerate(df['transactions']):
        for t in trans_list:
            X_single[i, transaction_map[t]] += 1
    
    # Добавляем признаки для всех возможных пар
    pairs = list(combinations(all_transactions, 2))
    X_pairs = np.zeros((len(df), len(pairs)))
    
    print(f"\nАнализируем {len(pairs)} возможных пар транзакций...")
    for i, trans_list in enumerate(df['transactions']):
        for j, (t1, t2) in enumerate(pairs):
            if t1 in trans_list and t2 in trans_list:
                X_pairs[i, j] = 1
    
    # Объединяем все признаки
    X = np.hstack([X_single, X_pairs])
    y = df['target'].values
    
    # Разделяем данные на рост и падение
    up_indices = y == 1
    down_indices = y == 0
    
    print(f"\nСтатистика разделения данных:")
    print(f"Всего примеров: {len(y)}")
    print(f"Дней роста: {np.sum(up_indices)}")
    print(f"Дней падения: {np.sum(down_indices)}")
    
    if np.sum(up_indices) < 5 or np.sum(down_indices) < 5:
        raise ValueError("Недостаточно данных для анализа роста или падения (нужно минимум 5 примеров каждого типа)")
    
    print("\nАнализ дней роста...")
    up_model, up_importance = analyze_subset(X[up_indices], y[up_indices], transaction_map, pairs, transaction_counts)
    
    print("\nАнализ дней падения...")
    down_model, down_importance = analyze_subset(X[down_indices], y[down_indices], transaction_map, pairs, transaction_counts)
    
    # Сохраняем модели и важные данные
    print("\nСохраняем модели и данные...")
    model_data = {
        'up_model': up_model,
        'down_model': down_model,
        'transaction_map': transaction_map,
        'pairs': pairs
    }
    
    # Создаем директорию, если её нет
    if not os.path.exists('models'):
        os.makedirs('models')
    
    joblib.dump(model_data, 'models/price_movement_model.joblib')
    print("Модели сохранены в models/price_movement_model.joblib")
    
    return {
        'up_patterns': up_importance,
        'down_patterns': down_importance,
        'transaction_counts': transaction_counts
    }

def analyze_subset(X, y, transaction_map, pairs, transaction_counts):
    """Анализирует подмножество данных (рост или падение)"""
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=5,
        min_samples_leaf=5,
        random_state=42
    )
    model.fit(X_train, y_train)
    
    feature_importance = {}
    
    # Анализ одиночных транзакций
    for t, idx in transaction_map.items():
        if model.feature_importances_[idx] > 0.01:
            feature_importance[f"single_{t}"] = {
                'transactions': [t],
                'importance': model.feature_importances_[idx],
                'frequency': transaction_counts[t]
            }
    
    # Анализ пар
    for i, (t1, t2) in enumerate(pairs):
        idx = len(transaction_map) + i
        if model.feature_importances_[idx] > 0.01:
            feature_importance[f"pair_{t1}_{t2}"] = {
                'transactions': [t1, t2],
                'importance': model.feature_importances_[idx],
                'frequency': min(transaction_counts[t1], transaction_counts[t2])
            }
    
    return model, feature_importance

if __name__ == "__main__":
    df = create_combined_dataset(
        'Global_functions/.csv/btc_big_movements_20250121.csv',
        'Whale_bots_transactions/.csv/whalebot_transactions.csv'
    )
    
    print("Начинаем анализ всех транзакций...")
    results = analyze_price_movements(df)
    
    print("\nПаттерны для роста цены:")
    for pattern, info in sorted(results['up_patterns'].items(), 
                              key=lambda x: x[1]['importance'], 
                              reverse=True)[:10]:
        print(f"\nПаттерн: {pattern}")
        print(f"Транзакции: {info['transactions']}")
        print(f"Важность: {info['importance']:.3f}")
        print(f"Частота появления: {info['frequency']}")
    
    print("\nПаттерны для падения цены:")
    for pattern, info in sorted(results['down_patterns'].items(), 
                              key=lambda x: x[1]['importance'], 
                              reverse=True)[:10]:
        print(f"\nПаттерн: {pattern}")
        print(f"Транзакции: {info['transactions']}")
        print(f"Важность: {info['importance']:.3f}")
        print(f"Частота появления: {info['frequency']}")
    
    print("\nПервые 10 строк датафрейма:")
    print(df.head(10))
    print("\nПоследние 10 строк датафрейма:")
    print(df.tail(10))
    
    print("\nРазмер датафрейма:", df.shape)
    print("\nКоличество дней с транзакциями:", df['transactions'].apply(len).value_counts()) 