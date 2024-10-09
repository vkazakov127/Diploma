# -*- coding: utf-8 -*-
# m09_gd_upload_async.py
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build
import os
from datetime import datetime as dt
import asyncio
import pprint
from m00_proc import json_dump, gd_clear_folder


async def gd_upload_asyncio(local_file_path: str, local_file_name: str, gd_parent_id: str):
    # Загрузка файла в Google Drive
    file_metadata = {'name': local_file_name, 'parents': [gd_parent_id]}
    media = MediaFileUpload(os.path.join(local_file_path, local_file_name), resumable=True)
    try:
        created_id = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    except:
        print(f'File {local_file_name} is NOT uploaded.')
    else:
        print(f'{local_file_name} uploaded')


async def main():
    file_path1 = "01_test_files"
    # Засекаем ОБЩЕЕ время
    print('----------------- Start -------------------')
    # ---------
    pp = pprint.PrettyPrinter(indent=4)
    # ---------- Загрузка файлов в Google Drive
    du009_upload = {}  # Словарь на выход: результаты измерений
    print('----- Загрузка файлов в Google Drive ------')
    start_upload_time = dt.now()  # Засекаем время начала загрузки
    # Список файлов локальных, для загрузки в Google Drive
    files_to_upload = [d.name for d in os.scandir(file_path1) if d.is_file()]
    file_count = len(files_to_upload)  # Количество файлов для загрузки
    # Файл для записи измерений в json-файл
    json_nm = "du009_2_gd_upload_async.json"
    # Первая строка в json-файле, комментарий
    du009_upload['Start'] = f'Upload to Google Drive by asyncio. Number of files={file_count}'
    tasks1 = []  # Список задач asyncio
    for name_i in files_to_upload:
        # --- Загрузка файла в Google Drive
        print(f'{name_i}')
        task1 = asyncio.create_task(gd_upload_asyncio(file_path1, name_i, PARENT_ID))
        tasks1.append(task1)
    # ------- Управление задачами asyncio
    for t in tasks1:
        await t
    # ------ Это конец
    end_upload_time = dt.now()  # Засекаем время окончания загрузки
    upload_duration = end_upload_time - start_upload_time
    print(f'download_duration = {upload_duration} Sec')
    du009_upload['Stop'] = (f'-------- The End. {str(upload_duration)} Sec, '
                            f'file_count={file_count}')
    # Записываем в json-файл
    json_dump(du009_upload, json_nm)
    print('------------ The End ------------')


SCOPES = ['https://www.googleapis.com/auth/drive']  # See, edit, create, and delete all of your Google Drive files
SERVICE_ACCOUNT_FILE = "credentials.json"  # Ключ для сервисного аккаунта
PARENT_ID = '11sc1DYH5I0kAm2pNiTj1f_ug2DOKFYUT'  # parent folder for upload/download in the Google Drive
# Учётные данные
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# Ресурс для обращения к API, то есть это некая абстракция над REST API Drive, чтобы удобнее обращаться к API.
service = build('drive', 'v3', credentials=credentials)

# ---------- Сначала очистим папку в Google Drive
du009_clear = {}  # Словарь на выход: результаты измерений
print('------ Очистка папки в Google Drive -------')
# Файл для записи измерений в json-файл
json_name = "du009_1_gd_folder_clear.json"
# Первая строка в json-файле, комментарий
du009_clear['Start'] = f'Clear folder in Google Drive by API Google Drive. Number of files=unknown'
start_clear_time = dt.now()  # Засекаем время начала
files_found, files_deleted = gd_clear_folder(service)
end_clear_time = dt.now()  # Засекаем время окончания
duration_clear = end_clear_time - start_clear_time
print(f'files_deleted / files_found = {files_deleted} / {files_found}')
du009_clear['Stop'] = (f'-------- The End. {str(duration_clear)} Sec, '
                       f'files_deleted / files_found = {files_deleted}/{files_found} files')
# Записываем в json-файл
file_closed = json_dump(du009_clear, json_name)

# Тут происходит асинхронный запуск main()
asyncio.run(main())
