import logging
import pymysql
import os
import threading
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
from queue import Queue
import time

# Импортируем необходимые модули Tkinter
import tkinter as tk
from tkinter import filedialog, messagebox

DB_CONFIG = {
    'host': '217.12.40.214',
    'user': 'websen9w_parser',
    'password': 'FAwooxqZj!B8',
    'database': 'websen9w_parser',
    'charset': 'utf8mb4'
}

# Настройка логирования
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s', 
    encoding='utf-8',
    handlers=[
        logging.FileHandler("parser.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Параметры многопоточности
NUM_THREADS = 2  # Начните с 2 потоков и увеличивайте при необходимости

# Очередь для ссылок и лока для синхронизации
url_queue = Queue()
lock = threading.Lock()

# Счётчики
total_links = 0
processed_links = 0

# Глобальная переменная для пути к chromedriver
chromedriver_path = ''

def load_urls(file_path):
    global total_links
    with open(file_path, 'r', encoding='utf-8') as file:
        urls = [line.strip() for line in file if line.strip()]
        total_links = len(urls)
        for url in urls:
            url_queue.put(url)
    logging.info(f"Всего ссылок для обработки: {total_links}")

def connect_db():
    return pymysql.connect(**DB_CONFIG)

def clean_data(data):
    for key in data:
        if data[key] is None:
            data[key] = ''
    return data

def save_to_db(data, url):
    global processed_links
    with lock:
        try:
            data = clean_data(data)
            logging.info(f"Сохранение данных для {url}")
            
            connection = connect_db()
            cursor = connection.cursor()
            sql = """
            INSERT INTO documents (
                url, document_type, registration_number, valid_from, valid_to,
                certification_body, applicant, manufacturer, product, tn_ved_code,
                compliance_requirements, certificate_based_on, additional_info,
                issue_date, last_change_reason_status, shipping_documents, parsed_at
            )
            VALUES (
                %(url)s, %(document_type)s, %(registration_number)s, %(valid_from)s, %(valid_to)s,
                %(certification_body)s, %(applicant)s, %(manufacturer)s, %(product)s, %(tn_ved_code)s,
                %(compliance_requirements)s, %(certificate_based_on)s, %(additional_info)s,
                %(issue_date)s, %(last_change_reason_status)s, %(shipping_documents)s, %(parsed_at)s
            )
            ON DUPLICATE KEY UPDATE
                document_type = VALUES(document_type),
                registration_number = VALUES(registration_number),
                valid_from = VALUES(valid_from),
                valid_to = VALUES(valid_to),
                certification_body = VALUES(certification_body),
                applicant = VALUES(applicant),
                manufacturer = VALUES(manufacturer),
                product = VALUES(product),
                tn_ved_code = VALUES(tn_ved_code),
                compliance_requirements = VALUES(compliance_requirements),
                certificate_based_on = VALUES(certificate_based_on),
                additional_info = VALUES(additional_info),
                issue_date = VALUES(issue_date),
                last_change_reason_status = VALUES(last_change_reason_status),
                shipping_documents = VALUES(shipping_documents),
                parsed_at = VALUES(parsed_at)
            """
            data['url'] = url
            data['parsed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute(sql, data)
            connection.commit()
            cursor.close()
            connection.close()
            
            logging.info(f"Данные для {url} успешно сохранены в базу данных.")
            
            processed_links += 1
            logging.info(f"Обработано ссылок: {processed_links}/{total_links}. Осталось: {total_links - processed_links}")
        except Exception as e:
            logging.error(f"Ошибка при сохранении данных для {url}: {e}")

def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Запуск браузера в фоновом режиме
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    # Используем выбранный пользователем путь к chromedriver
    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver

def parse_page(driver, url):
    data = {}
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logging.info(f"Начало обработки страницы: {url}")
            driver.get(url)
            wait = WebDriverWait(driver, 20)
            
            def get_element_text(by, identifier):
                try:
                    element = wait.until(EC.presence_of_element_located((by, identifier)))
                    return element.text
                except Exception as e:
                    logging.warning(f"Не удалось найти элемент {identifier} на {url}. Ошибка: {e}")
                    return None

            data['document_type'] = get_element_text(By.ID, "reportDataForm:kindCode")
            data['registration_number'] = get_element_text(By.ID, "reportDataForm:registrationNumber")
            data['valid_from'] = convert_date_format(get_element_text(By.ID, "reportDataForm:validFrom"))
            data['valid_to'] = convert_date_format(get_element_text(By.ID, "reportDataForm:validTo"))
            data['certification_body'] = get_element_text(By.ID, "reportDataForm:boxCertAuth")
            data['applicant'] = get_element_text(By.ID, "reportDataForm:boxApplicant")
            data['manufacturer'] = get_element_text(By.ID, "reportDataForm:boxManufacturer")
            data['product'] = get_element_text(By.ID, "reportDataForm:boxProduct")
            data['tn_ved_code'] = get_element_text(By.ID, "reportDataForm:box")
            data['compliance_requirements'] = get_element_text(By.ID, "reportDataForm:boxCorrToReq")
            data['certificate_based_on'] = get_element_text(By.ID, "reportDataForm:boxIssuedOn")
            data['additional_info'] = get_element_text(By.ID, "reportDataForm:boxAddInf")
            data['issue_date'] = convert_date_format(get_element_text(By.ID, "reportDataForm:issueDate"))
            data['last_change_reason_status'] = get_element_text(By.ID, "reportDataForm:reason")
            data['shipping_documents'] = get_element_text(By.ID, "reportDataForm:shippingDocument")
            
            logging.info(f"Страница {url} успешно спарсена.")
            return data
        except Exception as e:
            logging.error(f"Ошибка при парсинге страницы {url} на попытке {attempt + 1}: {e}")
            time.sleep(5)  # Ожидание перед следующей попыткой
    return None

def convert_date_format(date_str):
    try:
        return datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None

def worker():
    driver = create_driver()
    while not url_queue.empty():
        url = url_queue.get()
        try:
            data = parse_page(driver, url)
            if data:
                save_to_db(data, url)
            time.sleep(1)  # Задержка между запросами
        except Exception as e:
            logging.error(f"Ошибка в потоке при обработке {url}: {e}")
        finally:
            url_queue.task_done()
    driver.quit()

def start_parsing(file_path):
    load_urls(file_path)
    
    threads = []
    for _ in range(NUM_THREADS):
        thread = threading.Thread(target=worker)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    
    logging.info("Парсинг завершён.")
    messagebox.showinfo("Готово", "Парсинг завершён.")

def main():
    global chromedriver_path
    # Создаем главное окно
    root = tk.Tk()
    root.withdraw()  # Скрываем главное окно

    messagebox.showinfo("Добро пожаловать", "Добро пожаловать в парсер.")
    
    # Открываем диалоговое окно для выбора файла chromedriver.exe
    messagebox.showinfo("Выбор chromedriver", "Пожалуйста, выберите файл chromedriver.exe")
    chromedriver_path = filedialog.askopenfilename(
        title="Выберите файл chromedriver.exe",
        filetypes=(("Chromedriver", "chromedriver.exe"), ("Все файлы", "*.*"))
    )

    if not chromedriver_path:
        messagebox.showerror("Ошибка", "Вы не выбрали файл chromedriver.exe. Программа будет закрыта.")
        return

    # Проверяем, существует ли файл chromedriver.exe
    if not os.path.exists(chromedriver_path):
        messagebox.showerror("Ошибка", f"Файл {chromedriver_path} не найден. Программа будет закрыта.")
        return

    # Открываем диалоговое окно для выбора файла со ссылками
    messagebox.showinfo("Выбор файла ссылок", "Пожалуйста, выберите .txt файл со ссылками.")
    file_path = filedialog.askopenfilename(
        title="Выберите файл со ссылками",
        filetypes=(("Текстовые файлы", "*.txt"), ("Все файлы", "*.*"))
    )

    if file_path:
        # Запускаем парсинг
        start_parsing(file_path)
    else:
        messagebox.showwarning("Файл не выбран", "Вы не выбрали файл. Программа будет закрыта.")
        return

if __name__ == "__main__":
    main()
