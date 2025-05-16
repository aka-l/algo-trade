import pandas as pd
import os
from datetime import timedelta

def merge_transactions():
    """Объединяет данные из всех файлов каналов в один общий файл"""
    # Читаем данные из файлов
    whalebot_df = pd.read_csv('whalebot_transactions.csv') if os.path.exists('whalebot_transactions.csv') else None
    whale_alert_df = pd.read_csv('whale_alert_transactions.csv') if os.path.exists('whale_alert_transactions.csv') else None
    
    if whalebot_df is not None and whale_alert_df is not None:
        print("\nИсходные данные:")
        print(f"Записей в whalebot_transactions.csv: {len(whalebot_df)}")
        print(f"Записей в whale_alert_transactions.csv: {len(whale_alert_df)}")
        
        # Преобразуем даты в datetime с сохранением времени
        whalebot_df['date'] = pd.to_datetime(whalebot_df['date'], utc=True)
        whale_alert_df['date'] = pd.to_datetime(whale_alert_df['date'], utc=True)
        
        # Добавляем имена ботов
        whalebot_df['bot_name'] = 'whalebot'
        whale_alert_df['bot_name'] = 'whale_alert'
        
        # Объединяем датафреймы
        merged_df = pd.concat([whalebot_df, whale_alert_df], ignore_index=True)
        print(f"\nПосле объединения всего записей: {len(merged_df)}")
        
        # Проверяем на дубликаты до начала обработки
        exact_duplicates = merged_df[merged_df.duplicated(['date', 'btc'], keep=False)]
        print(f"Точных дубликатов до обработки: {len(exact_duplicates)}")
        
        # Ищем похожие транзакции (одинаковая сумма BTC и время в пределах 3 минут)
        final_df = merged_df.copy()
        similar_transactions = []
        processed_pairs = set()  # Для отслеживания уже обработанных пар
        
        # Сортируем по дате для оптимизации поиска
        merged_df = merged_df.sort_values('date')
        
        print("\nНачинаем поиск похожих транзакций...")
        # Ищем похожие транзакции
        for i, row in merged_df.iterrows():
            if isinstance(i, int) and i % 1000 == 0:
                print(f"Обработано {i} записей...")
            
            # Пропускаем уже обработанные записи
            if i in processed_pairs:
                continue
                
            # Ищем транзакции в пределах 3 минут
            time_window = (row['date'] - timedelta(minutes=3), row['date'] + timedelta(minutes=3))
            similar = merged_df[
                (merged_df['date'].between(*time_window)) & 
                (merged_df['btc'] == row['btc']) &
                (merged_df.index != i)
            ]
            
            if not similar.empty:
                for _, similar_row in similar.iterrows():
                    # Проверяем, что транзакции от разных ботов
                    if row['bot_name'] != similar_row['bot_name']:
                        pair_key = (i, similar_row.name)
                        if pair_key not in processed_pairs:
                            similar_transactions.append((
                                [i, similar_row.name],
                                [row['bot_name'], similar_row['bot_name']]
                            ))
                            processed_pairs.add(pair_key)
                            processed_pairs.add(i)
                            processed_pairs.add(similar_row.name)
        
        print(f"\nНайдено {len(similar_transactions)} пар похожих транзакций")
        
        # Обрабатываем похожие транзакции
        processed_indices = set()
        merged_count = 0
        
        for indices, bot_names in similar_transactions:
            if not any(idx in processed_indices for idx in indices):
                merged_count += 1
                # Помечаем индексы как обработанные
                processed_indices.update(indices)
                # Объединяем имена ботов
                final_df.loc[indices[0], 'bot_name'] = ','.join(sorted(set(bot_names)))
                # Удаляем дубликаты
                final_df = final_df.drop(indices[1:])
                
                if merged_count % 1000 == 0:
                    print(f"Обработано {merged_count} пар транзакций")
        
        print(f"\nОбъединено {merged_count} пар транзакций")
        
        # Проверяем количество записей после обработки
        print("\nПроверка количества записей:")
        print(f"Было: {len(merged_df)}")
        print(f"Стало: {len(final_df)}")
        print(f"Разница: {len(merged_df) - len(final_df)}")
        
        # Теперь преобразуем даты в формат YYYY-MM-DD
        final_df['date'] = final_df['date'].dt.strftime('%Y-%m-%d')
        
        # Сортируем по дате
        final_df = final_df.sort_values('date')
        
        # Сохраняем результат
        final_df.to_csv('all_btc_transactions.csv', index=False)
        print(f"\nОбъединенные данные сохранены в all_btc_transactions.csv: {len(final_df)} записей")
        
        # Статистика по источникам
        print("\nСтатистика по источникам:")
        source_stats = final_df['bot_name'].value_counts()
        print(source_stats)
        print(f"\nОбщая сумма записей по статистике: {source_stats.sum()}")
        
        # Проверяем баланс
        expected = len(merged_df) - merged_count  # merged_count это количество объединенных пар
        print(f"\nОжидаемое количество записей: {expected}")
        print(f"Фактическое количество записей: {len(final_df)}")
        if expected != len(final_df):
            print("ВНИМАНИЕ: Количество записей не совпадает с ожидаемым!")
            print(f"Разница: {len(final_df) - expected}")
        
        return final_df
    else:
        print("Не найдены необходимые файлы с данными")
        return pd.DataFrame(columns=['date', 'btc', 'bot_name'])

if __name__ == "__main__":
    merge_transactions()