# -*- coding: utf-8 -*-
# m08_gd_down_multiproc.py
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build
from datetime import datetime as dt
import pprint
import os
import io
import multiprocessing as mp
from m00_proc import json_dump, format_bytes, clear_folder
from m00_proc import gd_get_file_list, gd_download_file


def gd_download_file_by_pool(gd_file_dict: dict) -> bool:
    gd_file_id = gd_file_dict['id']
    local_file_name = gd_file_dict['name']
    # Запрос на скачивание файла
    request = service.files().get_media(fileId=gd_file_id)
    # Скачать в папку "file_path2" с именем "local_file_name"
    filename2 = os.path.join(file_path2, local_file_name)
    fh = io.FileIO(filename2, 'wb')
    # Скачивание пошло
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    attempts_number = 10  # Количество попыток скачать файл
    while (not done) and attempts_number:
        try:
            status, done = downloader.next_chunk()
        except:
            attempts_number -= 1
            continue
        else:
            print(f'Download: {local_file_name}, {int(status.progress() * 100)} %')
        finally:
            return done and attempts_number


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
# Ресурс для обращения к API, то есть это некая абстракция над REST API Drive, чтобы обращаться к методам API.
service = build('drive', 'v3', credentials=credentials)

if __name__ == '__main__':
    # ---------- Получение списка файлов, pageSize=1000
    print('--------------------- Start ------------------------')
    print('----- Получение списка файлов из Google Drive ------')
    filelist = gd_get_file_list(service)  # Получение списка файлов из Google Drive
    file_count = len(filelist)  # Количество найденных файлов в Google Drive
    # service, gd_file_id, gd_file_name
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
    print('----- а также multiprocessing ----------------------')
    # Файл для записи измерений в json-файл
    json_name = "du008_gd_down_multiproc.json"
    du008_down_multiproc = {}  # Словарь на выход: результаты измерений
    start_download_time = dt.now()  # Засекаем время начала загрузки
    # Первая строка в json-файле, комментарий
    du008_down_multiproc['Start'] = (f'Download files from Google Drive by multiprocessing. '
                                     f'Number of files={file_count}')
    # Цикл по полученному списку файлов
    start_time_whole = dt.now()  # Засекаем ОБЩЕЕ время
    print('---- Run process pool')
    with mp.Pool(processes=50) as pool:
        # Тут идут процессы, 50 штук
        # gd_download_file_by_pool — спец. версия из m00_proc.gd_download_file
        # filelist — список словарей,типа такого
        """
        {'id': '1yu1h_GsJhJAYZmZyGFBP4ghOc-iGHbgL',
        'mimeType': 'text/plain',
        'name': 'f00100.txt'}
        """
        # Тут идёт пул процессов, 50 штук
        pool.map(gd_download_file_by_pool, filelist)
    print('---- End process pool')
    # Это конец пула процессов.
    # Измеряем общее duration — интервал времени
    end_time_whole = dt.now()  # Засекаем ОБЩЕЕ время
    duration_whole = end_time_whole - start_time_whole
    #
    file_size_whole = sum(d.stat().st_size for d in os.scandir(file_path2) if d.is_file())
    du008_down_multiproc['Stop'] = (f'-------- The End. {str(duration_whole)} Sec, '
                                    f'{file_size_whole} Bytes = {format_bytes(file_size_whole)}')
    # Записываем в json-файл
    file_closed = json_dump(du008_down_multiproc, json_name)

    print(f'duration_whole = {duration_whole} Sec, {file_size_whole} Bytes = {format_bytes(file_size_whole)}')
    print('-------- The End ---------')
