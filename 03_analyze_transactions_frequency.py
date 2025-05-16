import pandas as pd
import matplotlib.pyplot as plt

def analyze_btc_transactions_frequency(df=None):
    """Анализирует частоту транзакций различных сумм BTC"""
    # Читаем файл, если датафрейм не передан
    if df is None:
        df = pd.read_csv('all_btc_transactions.csv')
    
    # Группируем по сумме BTC и подсчитываем частоту
    btc_frequency = df.value_counts('btc').reset_index()
    btc_frequency.columns = ['btc', 'frequency']
    
    # Сортируем по частоте по убыванию
    btc_frequency = btc_frequency.sort_values('frequency', ascending=False)
    
    print("Топ-10 самых частых сумм транзакций:")
    print(btc_frequency.head(10))
    
    # Создаём график
    plt.figure(figsize=(12, 6))
    plt.bar(range(len(btc_frequency)), btc_frequency['frequency'])
    
    # Настраиваем оси
    plt.xlabel('Сумма BTC (порядковый номер)')
    plt.ylabel('Количество транзакций')
    plt.title('Частота транзакций различных сумм BTC')
    
    # Добавляем текст с общей статистикой
    total_transactions = btc_frequency['frequency'].sum()
    unique_amounts = len(btc_frequency)
    plt.text(0.95, 0.95, 
             f'Всего транзакций: {total_transactions:,}\n'
             f'Уникальных сумм: {unique_amounts:,}',
             transform=plt.gca().transAxes,
             verticalalignment='top',
             horizontalalignment='right',
             bbox=dict(facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    plt.show()
    
    # Сохраняем результаты в CSV
    btc_frequency.to_csv('btc_frequency_analysis.csv', index=False)
    print("\nРезультаты анализа сохранены в btc_frequency_analysis.csv")
    
    return btc_frequency

if __name__ == "__main__":
    analyze_btc_transactions_frequency() 