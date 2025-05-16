from telethon import TelegramClient, events
from datetime import datetime
import pandas as pd
import re
import os
from dotenv import load_dotenv
import asyncio
from termcolor import cprint
import numpy as np

# Загружаем переменные окружения
load_dotenv()

# Конфигурация каналов и дат
DEFAULT_START_DATE = datetime(2017, 1, 1)

CHANNELS = {
    'whalebot': {
        'url': 'https://t.me/whalebotalerts',
        'pattern': r'(\d+(?:\.\d+)?)\s*BTC',
        'created_date': datetime(2023, 1, 1),
        'color': 'red'
    },
    'whale_alert': {
        'url': 'https://t.me/whale_alert_io',
        'pattern': r'(\d+(?:\.\d+)?)\s*#BTC',
        'created_date': datetime(2022, 1, 1),
        'color': 'blue'
    }
}

# Создаем DataFrame для каждого бота
dfs = {
    'whalebot': pd.DataFrame(columns=['date', 'btc']),
    'whale_alert': pd.DataFrame(columns=['date', 'btc'])
}

# Конфигурация отслеживаемых транзакций
WATCHED_TRANSACTIONS = {
    'whalebot': ['547 BTC', '840 BTC', '960 BTC'],
    'whale_alert': ['547 #BTC', '840 #BTC', '960 #BTC']
}

async def process_message(message, date, channel_name):
    pattern = CHANNELS[channel_name]['pattern']
    matches = re.search(pattern, message)
    
    # Извлекаем информацию о транзакции
    from_to_pattern = r'from\s+(\w+)\s+to\s+(\w+)'
    from_to_match = re.search(from_to_pattern, message, re.IGNORECASE)
    
    if matches:
        btc_amount = float(matches.group(1))
        
        # Формируем сообщение о транзакции
        if from_to_match:
            sender = from_to_match.group(1)
            receiver = from_to_match.group(2)
            formatted_message = f"{btc_amount} BTC {sender} → {receiver} ({channel_name})"
        else:
            formatted_message = f"{btc_amount} BTC ({channel_name})"
        
        # Выводим сообщение в соответствующем цвете
        cprint(formatted_message, CHANNELS[channel_name]['color'])
        
        new_row = pd.DataFrame({
            'date': [date],
            'btc': [btc_amount]
        })
        dfs[channel_name] = pd.concat([dfs[channel_name], new_row], ignore_index=True)
        return True
    return False

def save_channel_data(channel_name, new_df):
    """Сохранение новых данных канала в CSV файл с учетом времени транзакций"""
    csv_dir = './.csv'
    os.makedirs(csv_dir, exist_ok=True)
    filename = os.path.join('./.csv', f'{channel_name}_transactions.csv')
    
    # Создаем копию DataFrame чтобы избежать изменения оригинала
    new_df = new_df.copy()
    new_df['date'] = pd.to_datetime(new_df['date'])
    
    if os.path.exists(filename):
        try:
            existing_df = pd.read_csv(filename)
            # Преобразуем даты из существующего формата с format='mixed'
            existing_df['date'] = pd.to_datetime(existing_df['date'], format='mixed')
            
            if 'last_timestamp' in existing_df.columns:
                existing_df['last_timestamp'] = pd.to_datetime(existing_df['last_timestamp'], format='mixed')
                last_timestamp = existing_df['last_timestamp'].iloc[-1]
                last_timestamp = np.datetime64(last_timestamp)
            else:
                last_date = existing_df['date'].max()
                last_timestamp = pd.Timestamp.combine(last_date.date(), pd.Timestamp.max.time())
                last_timestamp = np.datetime64(last_timestamp)
            
            # Преобразуем даты в datetime64[ns]
            new_df['date'] = new_df['date'].astype('datetime64[ns]')
            
            # Фильтруем только новые записи
            new_records = new_df[new_df['date'] > last_timestamp]
            
            if not new_records.empty:
                # Подготавливаем новые записи, сохраняя полное время
                new_records_dates = new_records.copy()
                new_records_dates['date'] = new_records_dates['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
                
                # Создаем итоговый DataFrame
                if 'last_timestamp' in existing_df.columns:
                    base_df = existing_df[['date', 'btc']].iloc[:-1]
                else:
                    base_df = existing_df[['date', 'btc']].iloc[:-1]
                
                # Сохраняем полное время для последней записи
                last_record_date = pd.to_datetime(existing_df['date'].iloc[-1]).strftime('%Y-%m-%d %H:%M:%S')
                
                combined_df = pd.concat([
                    base_df,  # Старые записи без последней
                    pd.DataFrame({'date': [last_record_date], 'btc': [existing_df['btc'].iloc[-1]]}),  # Последняя старая запись
                    new_records_dates[['date', 'btc']]  # Новые записи
                ])
                
                # Добавляем колонку с последним timestamp, сохраняя полное время
                combined_df['last_timestamp'] = pd.NaT
                combined_df.loc[combined_df.index[-1], 'last_timestamp'] = new_records['date'].max().strftime('%Y-%m-%d %H:%M:%S')
                
                # Сохраняем результат
                combined_df.to_csv(filename, index=False)
                print(f"Добавлено {len(new_records)} новых записей")
        
        except Exception as e:
            print(f"Ошибка при обработке файла {filename}: {str(e)}")
            raise
    else:
        # Если файл не существует, сохраняем с полным временем
        new_df_dates = new_df.copy()
        new_df_dates['date'] = new_df_dates['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        new_df_dates['last_timestamp'] = pd.NaT
        new_df_dates.loc[new_df_dates.index[-1], 'last_timestamp'] = new_df['date'].max().strftime('%Y-%m-%d %H:%M:%S')
        new_df_dates[['date', 'btc', 'last_timestamp']].to_csv(filename, index=False)
    
    return filename

async def process_batch(messages, channel_name):
    btc_found = 0
    processed_transactions = set()
    
    for message in messages:
        if message.text and ('BTC' in message.text or '#BTC' in message.text):
            btc_match = re.search(CHANNELS[channel_name]['pattern'], message.text)
            if btc_match:
                btc_amount = float(btc_match.group(1))
                # Преобразуем время сообщения в tz-naive формат
                message_timestamp = pd.Timestamp(message.date).tz_localize(None)
                
                # Создаем уникальный ключ для транзакции с точностью до минуты
                transaction_key = f"{message_timestamp.strftime('%Y-%m-%d %H:%M')}_{btc_amount}"
                
                if transaction_key in processed_transactions:
                    continue
                    
                processed_transactions.add(transaction_key)
                
                # Проверяем наличие дубликатов в существующем DataFrame
                if not dfs[channel_name].empty:
                    df_dates = pd.to_datetime(dfs[channel_name]['date'])
                    time_diff = (df_dates - message_timestamp).abs()
                    mask = (dfs[channel_name]['btc'] == btc_amount) & (time_diff <= pd.Timedelta(minutes=1))
                    if mask.any():
                        continue

                # Сохраняем транзакцию в DataFrame
                new_row = pd.DataFrame({
                    'date': [message_timestamp],
                    'btc': [btc_amount]
                })
                dfs[channel_name] = pd.concat([dfs[channel_name], new_row], ignore_index=True)
                btc_found += 1

                # Остальной код вывода информации
                from_to_match = re.search(r'from\s+(\w+)\s+to\s+(\w+)', message.text, re.IGNORECASE)
                if from_to_match:
                    sender = from_to_match.group(1)
                    receiver = from_to_match.group(2)
                    output = f"{btc_amount} BTC {sender} → {receiver} ({channel_name})"
                else:
                    output = f"{btc_amount} BTC ({channel_name})"
                
                cprint(output, CHANNELS[channel_name]['color'])

    if btc_found > 0:
        save_channel_data(channel_name, dfs[channel_name])
        print(f"💾 Пакет обработан. Найдено BTC: {btc_found}")

async def get_history():
    print("Получаем историю сообщений...")
    
    for channel_name, channel_info in CHANNELS.items():
        # Используем ./.csv для работы в текущем каталоге
        filename = os.path.join('./.csv', f'{channel_name}_transactions.csv')
        
        if os.path.exists(filename):
            existing_df = pd.read_csv(filename)
            existing_df['date'] = pd.to_datetime(existing_df['date'], format='mixed')
            last_date = existing_df['date'].max()
            current_time = pd.Timestamp.now()
            
            print(f"\nПроверка канала {channel_name}:")
            print(f"Найден файл {filename}")
            print(f"Существующие записи: {len(existing_df)}")
            print(f"Первая дата в файле: {existing_df['date'].min()}")
            print(f"Последняя дата в файле: {last_date}")
            
            if (current_time - last_date).days < 1:
                print(f"Канал {channel_name} уже обработан до текущей даты. Пропускаем...")
                continue
            
            # Инициализируем DataFrame существующими данными
            dfs[channel_name] = existing_df.copy()
            print(f"Загружено {len(dfs[channel_name])} существующих записей")
        else:
            last_date = CHANNELS[channel_name]['created_date']
            print(f"\nНачинаем сбор данных канала {channel_name} с {last_date}")
        
        print(f"Обработка канала {channel_name}...")
        messages_count = 0
        messages_batch = []
        
        async for message in client.iter_messages(str(channel_info['url']), 
                                                offset_date=last_date,
                                                reverse=True):
            messages_count += 1
            messages_batch.append(message)
            
            if len(messages_batch) >= 100:
                print(f"Обработка пакета {messages_count-99}-{messages_count}")
                await process_batch(messages_batch, channel_name)
                messages_batch = []
        
        if messages_batch:
            await process_batch(messages_batch, channel_name)
        
        print(f"Канал {channel_name} обработан. Всего сообщений: {messages_count}")
    
    print("\nВсе каналы обработаны!")

async def main():
    try:
        print("Начинаем мониторинг BTC транзакций...")
        await get_history()
        print("\nПереходим к мониторингу новых сообщений...")
        print("\nОжидаем новые транзакции...")
        
        @client.on(events.NewMessage(chats=[info['url'] for info in CHANNELS.values()]))
        async def handle_new_message(event):
            try:
                message = event.message
                
                channel_url = str(event.message.peer_id.channel_id)
                channel_name = None
                for name, info in CHANNELS.items():
                    if channel_url in info['url'] or info['url'] in channel_url:
                        channel_name = name
                        break

                if message.text and ('BTC' in message.text or '#BTC' in message.text):
                    btc_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:#)?BTC', message.text)
                    if btc_match:
                        btc_amount = float(btc_match.group(1))
                        from_to_match = re.search(r'from\s+(\w+)\s+to\s+(\w+)', message.text, re.IGNORECASE)
                        
                        if from_to_match:
                            sender = from_to_match.group(1)
                            receiver = from_to_match.group(2)
                            output = f"{btc_amount} BTC {sender} → {receiver} ({channel_name})"
                            
                            # Проверяем, является ли транзакция отслеживаемой
                            watched_amount = f"{int(btc_amount)} BTC"
                            watched_amount_hash = f"{int(btc_amount)} #BTC"
                            
                            if (watched_amount in WATCHED_TRANSACTIONS[channel_name] if channel_name in WATCHED_TRANSACTIONS else [] or 
                                watched_amount_hash in WATCHED_TRANSACTIONS[channel_name] if channel_name in WATCHED_TRANSACTIONS else []):
                                # Особый вывод для отслеживаемых транзакций
                                stars = '*' * 3
                                attrs = ['bold', 'blink']
                                highlighted_output = f"{stars}{output}"
                                for _ in range(4):
                                    cprint(highlighted_output, 'white', 'on_red', attrs=attrs)
                                print('')  # Пустая строка после важной транзакции
                            else:
                                # Обычный вывод с мягким фоном
                                cprint(output, 'white', 'on_cyan')
                        
            except Exception as e:
                print(f"Ошибка при обработке сообщения: {str(e)}")

        await client.run_until_disconnected()
        
    except Exception as e:
        print(f"Произошла ошибка: {str(e)}")
    finally:
        if client.is_connected():
            await client.disconnect()

# Создаем клиент
client = TelegramClient(
    'crypto_monitor', 
    api_id=int(os.getenv('API_ID', 0)),
    api_hash=str(os.getenv('API_HASH', ''))
)

if __name__ == "__main__":
    try:
        with client:
            client.loop.run_until_complete(main())
    except KeyboardInterrupt:
        print("\nВыход из программы...")