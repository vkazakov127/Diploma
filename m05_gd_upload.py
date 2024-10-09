# -*- coding: utf-8 -*-
# m05_gd_upload.py
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build
import pprint
from datetime import datetime as dt
import os
from m00_proc import json_dump, format_bytes, gd_get_file_list


def gd_clear_folder():  # Очистить папку в Google Drive
    filelist = gd_get_file_list(service)  # Получение списка файлов из Google Drive
    files_found_cnt = len(filelist)  # Количество найденных файлов в Google Drive
    files_deleted_cnt = 0
    for file_i in filelist:
        file_id = file_i['id']
        # Удалить файл в Google Drive
        try:
            service.files().delete(fileId=file_id).execute()
        except:
            print(f"File {file_i['name']} could not be deleted")
        else:
            print(f"File {file_i['name']} has been deleted")
            files_deleted_cnt += 1
    return files_found_cnt, files_deleted_cnt  # Успешно ли удалены все файлы


def gd_upload_file(local_file_path: str, local_file_name: str, gd_parent_id: str):  # Загрузка файла в Google Drive
    file_metadata = {'name': local_file_name, 'parents': [gd_parent_id]}
    media = MediaFileUpload(os.path.join(local_file_path, local_file_name), resumable=True)
    try:
        created_id = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    except:
        return '0'
    else:
        return created_id


# --------- Настройки ------------
file_path1 = "01_test_files"
# Засекаем ОБЩЕЕ время
print('----------------- Start -------------------')
print('------ Список файлов на Google Drive ------')
# ---------
pp = pprint.PrettyPrinter(indent=4)

SCOPES = ['https://www.googleapis.com/auth/drive']  # See, edit, create, and delete all of your Google Drive files
SERVICE_ACCOUNT_FILE = "credentials.json"  # Ключ для сервисного аккаунта
PARENT_ID = '11sc1DYH5I0kAm2pNiTj1f_ug2DOKFYUT'  # parent folder for upload/download in the Google Drive
# Учётные данные
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
# Ресурс для обращения к API, то есть это некая абстракция над REST API Drive, чтобы удобнее обращаться к методам API.
service = build('drive', 'v3', credentials=credentials)

# ---------- Сначала очистим папку в Google Drive
du005_clear = {}  # Словарь на выход: результаты измерений
print('------ Очистка папки в Google Drive -------')
# Файл для записи измерений в json-файл
json_name = "du005_1_gd_folder_clear.json"
# Первая строка в json-файле, комментарий
du005_clear['Start'] = f'Clear folder in Google Drive by API Google Drive. Number of files=unknown'
start_clear_time = dt.now()  # Засекаем время начала
files_found, files_deleted = gd_clear_folder()
end_clear_time = dt.now()  # Засекаем время окончания
duration_clear = end_clear_time - start_clear_time
print(f'files_deleted / files_found = {files_deleted} / {files_found}')
du005_clear['Stop'] = (f'-------- The End. {str(duration_clear)} Sec, '
                       f'files_deleted / files_found = {files_deleted}/{files_found} files')
# Записываем в json-файл
file_closed = json_dump(du005_clear, json_name)

# ---------- Загрузка файлов в Google Drive
du005_upload = {}  # Словарь на выход: результаты измерений
file_size_whole = 0  # Общий размер всех записанных файлов, в Байтах
print('----- Загрузка файлов в Google Drive ------')
start_upload_time = dt.now()  # Засекаем время начала загрузки
# Список файлов локальных, для загрузки в Google Drive
files_to_upload = [d.name for d in os.scandir(file_path1) if d.is_file()]
file_count = len(files_to_upload)  # Количество файлов для загрузки
# Файл для записи измерений в json-файл
json_name = "du005_3_gd_upload_api.json"
# Первая строка в json-файле, комментарий
du005_upload['Start'] = f'Upload to Google Drive by API Google Drive. Number of files={file_count}'

# gd_upload_file(local_file_path: str, local_file_name: str, gd_parent_id: str)
files_uploaded_cnt = 0  # Количество успешно загруженных файлов
for name_i in files_to_upload:
    # --- Загрузка файла в Google Drive
    print(f'{name_i}')
    gd_id = gd_upload_file(file_path1, name_i, PARENT_ID)
    if gd_id == '0':
        print(f'File {name_i} is not uploated')
    else:
        print(f'{name_i}, created_id={gd_id}')
        files_uploaded_cnt += 1
# ------ Это конец
print('---------- Запись результатов -------------')
# Без multiproc: upload_duration = 0:08:10.064289 Sec
end_upload_time = dt.now()  # Засекаем время окончания загрузки
upload_duration = end_upload_time - start_upload_time
print(f'upload_duration = {upload_duration} Sec')
print(f'files_uploaded / files_to_upload = {files_uploaded_cnt} / {file_count}')
# Подсчёты для статистики в json
file_size_whole = sum(d.stat().st_size for d in os.scandir(file_path1) if d.is_file())
du005_upload['Stop'] = (f'-------- The End. {str(upload_duration)} Sec, '
                        f'files_uploaded / files_to_upload = {files_uploaded_cnt} / {file_count}, '
                        f'{file_size_whole} Bytes = {format_bytes(file_size_whole)}')
# Записываем в json-файл
file_closed = json_dump(du005_upload, json_name)
print('---------- The End -----------')
