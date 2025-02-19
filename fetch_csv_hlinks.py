import csv
import requests
from bs4 import BeautifulSoup
import time
import argparse
from multiprocessing import Pool, cpu_count

def get_article_text(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = response.apparent_encoding  # Автоопределение кодировки
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Попытка извлечь основной текст статьи
        article_tags = ["article", "main", "div"]
        for tag in article_tags:
            article = soup.find(tag)
            if article:
                paragraphs = article.find_all("p")
                text = "\n".join(p.get_text(strip=True) for p in paragraphs)
                if text:
                    return text
        
        return "Не удалось извлечь текст"
    except Exception as e:
        return f"Ошибка: {e}"

def process_url(args):
    url, index = args
    print(f"Обрабатываю [{index}]: {url}")
    text = get_article_text(url)
    time.sleep(1)  # Пауза между запросами
    return [url, text]

def process_csv(input_file, output_file):
    with open(input_file, "r", newline="", encoding="utf-8-sig") as infile:
        reader = csv.reader(infile)
        header = next(reader)  # Читаем заголовок
        urls = [(row[0], idx) for idx, row in enumerate(reader) if row]

    # Определяем количество процессов (используем половину доступных ядер)
    num_processes = max(1, cpu_count() // 2)
    print(f"Запуск обработки с использованием {num_processes} процессов")

    # Создаем пул процессов и обрабатываем URLs
    with Pool(num_processes) as pool:
        results = pool.map(process_url, urls)

    # Записываем результаты в выходной файл
    with open(output_file, "w", newline="", encoding="utf-8-sig") as outfile:
        writer = csv.writer(outfile)
        header.append("Article Text")
        writer.writerow(header)
        
        # Сортируем результаты по индексу для сохранения порядка
        for url, text in sorted(results, key=lambda x: x[0]):
            writer.writerow([url, text])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape articles from CSV links")
    parser.add_argument("-i", required=True, metavar="INPUT", help="Path to input CSV file")
    parser.add_argument("-o", default="output.csv", metavar="OUTPUT", help="Path to output CSV file")
    args = parser.parse_args()
    
    process_csv(args.i, args.o)
