# -*- coding: utf-8 -*-
# m06_gd_down_async.py
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build
import pprint
import io
from datetime import datetime as dt
import os
import asyncio
from m00_proc import json_dump, clear_folder, gd_get_file_list


async def gd_download_file(gd_file_id: str, local_file_path: str, local_file_name: str):
    request = service.files().get_media(fileId=gd_file_id)
    # Скачать в папку "local_file_path" с именем "local_file_name"
    filename2 = os.path.join(local_file_path, local_file_name)
    fh = io.FileIO(filename2, 'wb')
    # Скачивание пошло
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    attempts_number = 10
    while (not done) and attempts_number:
        try:
            status, done = downloader.next_chunk()
        except:
            attempts_number -= 1
            continue
        else:
            print(f'Download: {local_file_name}, {int(status.progress() * 100)} %')


async def main():
    # ---------- Скачивание файлов из Google Drive
    print('----- Скачивание файлов из Google Drive ------------')
    print('----- используя API Google Drive -------------------')
    print('----- а также asyncio.Task -------------------------')
    # Файл для записи измерений в json-файл
    json_name = "du006_gd_down_async.json"
    du006_gd_down_async = {}  # Словарь на выход: результаты измерений
    # Очищаем локальную папку-приёмник файлов. Или создаём её, если она ещё не существует.
    clear_folder(file_path2)
    # Первая строка в json-файле, комментарий
    du006_gd_down_async[
        'Start'] = f'Download files from Google Drive by asyncio. Number of files={file_count}'

    start_download_time = dt.now()  # Засекаем время начала загрузки
    tasks1 = []  # Список задач asyncio
    for file_i in filelist:
        file_id = file_i['id']
        file_name = file_i['name']
        # Задача на скачивание файла из Google Drive
        task1 = asyncio.create_task(gd_download_file(file_id, file_path2, file_name))
        tasks1.append(task1)
    # ------- Управление задачами asyncio
    for t_i in tasks1:
        await t_i
    # ------- Это конец
    end_download_time = dt.now()  # Засекаем время окончания загрузки
    download_duration = end_download_time - start_download_time
    print(f'download_duration = {download_duration} Sec')
    du006_gd_down_async['Stop'] = (f'-------- The End. {str(download_duration)} Sec, '
                                      f'file_count={file_count}')
    # Записываем в json-файл
    json_dump(du006_gd_down_async, json_name)
    print('------------ The End ------------')


# --------- Настройки ------------
file_path1 = "01_test_files"  # Для загрузки в Google Drive
file_path2 = "02_test_files"  # Для скачивания из Google Drive
# ---------
SCOPES = ['https://www.googleapis.com/auth/drive']  # See, edit, create, and delete all of your Google Drive files
SERVICE_ACCOUNT_FILE = "credentials.json"  # Ключ для сервисного аккаунта
PARENT_ID = '11sc1DYH5I0kAm2pNiTj1f_ug2DOKFYUT'  # parent folder for upload/download in the Google Drive
# Учётные данные
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# Ресурс для обращения к API, то есть это некая абстракция над REST API Drive, чтобы удобнее обращаться к методам API.
service = build('drive', 'v3', credentials=credentials)

print('--------------------- Start ------------------------')
print('----- Получение списка файлов из Google Drive ------')
pp = pprint.PrettyPrinter(indent=4)
# ---------- Получение списка файлов, pageSize=1000
filelist = gd_get_file_list(service)  # Получение списка файлов из Google Drive
file_count = len(filelist)  # Количество найденных файлов в Google Drive
# Вывод на консоль полученного списка файлов
pp.pprint(filelist)

# Тут происходит асинхронный запуск main()
asyncio.run(main())
