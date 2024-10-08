# -*- coding: utf-8 -*-
# m05_gd_download.py
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build
import pprint
import io
from datetime import datetime as dt
import os
from m00_proc import json_dump, format_bytes, clear_folder
from m00_proc import gd_get_file_list, gd_download_file


# --------- Настройки ------------
file_path1 = "01_test_files"  # Для загрузки в Google Drive
file_path2 = "02_test_files"  # Для скачивания из Google Drive
# json-файл для записи измерений
json_name = "du005_1_gd_files_list_api.json"
du005_dict = {}  # Словарь на выход
# Засекаем время на получение файлов
print('--------------------- Start ------------------------')
print('----- Получение списка файлов из Google Drive ------')
start_time_whole = dt.now()
# ---------
pp = pprint.PrettyPrinter(indent=4)

SCOPES = ['https://www.googleapis.com/auth/drive']  # See, edit, create, and delete all of your Google Drive files
SERVICE_ACCOUNT_FILE = "credentials.json"  # Ключ для сервисного аккаунта
PARENT_ID = '11sc1DYH5I0kAm2pNiTj1f_ug2DOKFYUT'  # parent folder for upload/download in the Google Drive
# Учётные данные
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# Ресурс для обращения к API, то есть это некая абстракция над REST API Drive, чтобы удобнее обращаться к методам API.
service = build('drive', 'v3', credentials=credentials)


du005_dict['Start'] = f'Get files list from Google Drive by API Google Drive. Number of files=unknown'
# ---------- Получение списка файлов, pageSize=1000
filelist = gd_get_file_list(service)  # Получение списка файлов из Google Drive
file_count = len(filelist)  # Количество найденных файлов в Google Drive
# Измеряем ОБЩЕЕ duration — интервал времени
end_time_whole = dt.now()  # Засекаем ОБЩЕЕ время
duration_whole = end_time_whole - start_time_whole
# Вывод на консоль
pp.pprint(filelist)
print('------------------------------')
print(f'duration_whole = {duration_whole} Sec')
du005_dict['Stop'] = (f'-------- The End. {str(duration_whole)} Sec, '
                          f'file_count={file_count}')
# Записываем в json-файл
file_closed = json_dump(du005_dict, json_name)

# ---------- Скачивание файлов из Google Drive
print('-------- Скачивание файлов из Google Drive ---------')
# Файл для записи измерений в json-файл
json_name = "du005_2_gd_down_api.json"
du005_dict = {}   # Словарь на выход: результаты измерений
# Очищаем локальную папку-приёмник файлов. Или создаём её, если она ещё не существует.
clear_folder(file_path2)
# Первая строка в json-файле, комментарий
du005_dict['Start'] = f'Download files from Google Drive by API Google Drive. Number of files={file_count}'

start_download_time = dt.now()  # Засекаем время начала загрузки
for file_i in filelist:
    file_id = file_i['id']
    file_name = file_i['name']
    # Скачивание файла из Google Drive
    success1 = gd_download_file(service, file_id, file_path2, file_name)
    if not success1:
        du005_dict['Exception'] = f'file_name={file_name}, file_id={file_id}, file_path2={file_path2}'
end_download_time = dt.now()  # Засекаем время окончания загрузки
download_duration = end_download_time - start_download_time
print(f'download_duration = {download_duration} Sec')
du005_dict['Stop'] = (f'-------- The End. {str(download_duration)} Sec, '
                          f'file_count={file_count}')
# Записываем в json-файл
file_closed = json_dump(du005_dict, json_name)
print('------------ The End ------------')

