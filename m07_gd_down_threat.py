# -*- coding: utf-8 -*-
# m07_gd_down_threat.py
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build
import pprint
from datetime import datetime as dt
from m00_proc import json_dump, clear_folder
from m00_proc import gd_get_file_list, gd_download_file
from threading import Thread

# --------- Настройки ------------
file_path1 = "01_test_files"  # Для загрузки в Google Drive
file_path2 = "02_test_files"  # Для скачивания из Google Drive

# Засекаем ОБЩЕЕ время
start_time_file_list = dt.now()
# ---------
pp = pprint.PrettyPrinter(indent=4)

SCOPES = ['https://www.googleapis.com/auth/drive']  # See, edit, create, and delete all of your Google Drive files
SERVICE_ACCOUNT_FILE = "credentials.json"  # Ключ для сервисного аккаунта
PARENT_ID = '11sc1DYH5I0kAm2pNiTj1f_ug2DOKFYUT'  # parent folder for upload/download in the Google Drive
# Учётные данные
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# Ресурс для обращения к API, то есть это некая абстракция над REST API Drive, чтобы удобнее обращаться к методам API.
service = build('drive', 'v3', credentials=credentials)

# ---------- Получение списка файлов, pageSize=1000
print('--------------------- Start ------------------------')
print('----- Получение списка файлов из Google Drive ------')
filelist = gd_get_file_list(service)  # Получение списка файлов из Google Drive
file_count = len(filelist)  # Количество найденных файлов в Google Drive
# Измеряем ОБЩЕЕ duration — интервал времени
end_time_file_list = dt.now()  # Засекаем ОБЩЕЕ время
duration_file_list = end_time_file_list - start_time_file_list
# Вывод на консоль списка файлов в Google Drive
pp.pprint(filelist)
print('------------------------------')
print(f'duration_file_list = {duration_file_list} Sec')

# Очищаем локальную папку-приёмник файлов. Или создаём её, если она ещё не существует.
clear_folder(file_path2)

# ---------- Скачивание файлов из Google Drive
print('----- Скачивание файлов из Google Drive ------------')
print('----- используя API Google Drive -------------------')
print('----- а также threading.Thread ---------------------')
# Файл для записи измерений в json-файл
json_name = "du007_gd_down_threads.json"
du007_down_thread = {}   # Словарь на выход: результаты измерений
start_download_time = dt.now()  # Засекаем время начала загрузки
# Первая строка в json-файле, комментарий
du007_down_thread['Start'] = f'Download files from Google Drive by Threads. Number of files={file_count}'

threads1 = []  # Список потоков
max_thread_cnt = 5  # Максимальное количество потоков
i1 = 0
i2 = i1 + max_thread_cnt
while i2 < file_count:
    for file_i in filelist[i1:i2]:
        file_id = file_i['id']
        file_name = file_i['name']
        # Объявление потока
        thread_i = Thread(target=gd_download_file, args=(service, file_id, file_path2, file_name))
        threads1.append(thread_i)  # Список потоков
    # start Threads
    for i in threads1:
        i.start()
    # join threads
    for i in threads1:
        i.join()
    # new iteration
    i1 = i2
    i2 = i1 + max_thread_cnt
    threads1 = []  # Список потоков

"""    
for file_i in filelist:
    file_id = file_i['id']
    file_name = file_i['name']
    # Скачивание файла из Google Drive
    thread_i = Thread(target=gd_download_file, args=(service, file_id, file_path2, file_name))
    threads1.append(thread_i)  # Список потоков
# start Threads
for i in threads1:
    i.start()
# join threads
for i in threads1:
    i.join()
"""
# ------- Это конец
end_download_time = dt.now()  # Засекаем время окончания загрузки
download_duration = end_download_time - start_download_time
du007_down_thread['Stop'] = (f'-------- The End. {str(download_duration)} Sec, '
                                     f'file_count={file_count}')
# Записываем в json-файл
file_closed = json_dump(du007_down_thread, json_name)

print(f'download_duration = {download_duration} Sec, file_count={file_count}')
print('-------- The End ---------')
