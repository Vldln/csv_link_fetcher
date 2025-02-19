import csv
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import time
import argparse

def get_article_text(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(url, headers=headers, timeout=10)
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
                    return text[:1000]  # Ограничение текста до 1000 символов
        
        return "Не удалось извлечь текст"
    except Exception as e:
        return f"Ошибка: {e}"

def process_csv(input_file, output_file):
    with open(input_file, "r", newline="", encoding="utf-8") as infile, open(output_file, "w", newline="", encoding="utf-8") as outfile:
        
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        header = next(reader)  # Читаем заголовок
        header.append("Article Text")  # Добавляем новую колонку
        writer.writerow(header)
        
        for row in reader:
            if row:
                url = row[0]
                print(f"Обрабатываю: {url}")
                article_text = get_article_text(url)
                row.append(article_text)
                writer.writerow(row)
                time.sleep(1)  # Пауза между запросами

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape articles from CSV links")
    parser.add_argument("-i", required=True, metavar="INPUT", help="Path to input CSV file")
    parser.add_argument("-o", default="output.csv", metavar="OUTPUT", help="Path to output CSV file")
    args = parser.parse_args()
    
    process_csv(args.i, args.o)
