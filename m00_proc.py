# -*- coding: utf-8 -*-
# m00_proc.py
import os
import shutil
import json
import io
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from googleapiclient.discovery import build
from random import choice


def json_dump(output_dic: dict, json_file_name: str) -> bool:
    with open(json_file_name, mode='w', encoding='utf8') as file:
        json.dump(output_dic, file)  # Записываем в файл json
    return file.closed


def clear_folder(file_path: str) -> None:
    # Очищаем папку-приёмник файлов (локальную)
    # Можно использовать и просто для создания такой папки
    try:
        shutil.rmtree(file_path)  # Удаляем папку и всё, что в ней
    except:
        pass
    os.mkdir(file_path)  # Вновь создаёт папку, но уже пустую


def format_bytes(size) -> str:  # Перевести Байты в Килобайты и т.д.
    # 2**10 = 1024
    power = 2 ** 10
    n = 0
    power_labels = {0: "", 1: "Kilo", 2: "Mega", 3: "Giga", 4: "Tera"}
    while size > power:
        size /= power
        n += 1
    return f'{round(size, 2)} {power_labels[n] + "bytes"}'


def gd_get_file_list(service) -> list:  # Получение списка файлов из Google Drive
    results = service.files().list(pageSize=1000,
                                   fields="nextPageToken, files(id, name, mimeType)",
                                   q="mimeType contains 'text'").execute()
    nextPgToken = results.get('nextPageToken')
    while nextPgToken:
        next_Page = service.files().list(pageSize=1000,
                                         fields="nextPageToken, files(id, name, mimeType)",
                                         q="mimeType contains 'text'",
                                         pageToken=nextPgToken).execute()
        nextPgToken = next_Page.get('nextPageToken')
        # Добавляем в список результатов
        results['files'] = results['files'] + next_Page['files']
    return results['files']  # Список файлов по фильтру: mimeType contains 'text'


# Скачивание файла из Google Drive, используя API Google Drive
def gd_download_file(service, gd_file_id: str, local_file_path: str, local_file_name: str) -> bool:
    request = service.files().get_media(fileId=gd_file_id)
    # Скачать в папку "local_file_path" с именем "local_file_name"
    filename2 = os.path.join(local_file_path, local_file_name)
    fh = io.FileIO(filename2, 'wb')
    # Скачивание пошло
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    attempts_number = 10   # Количество попыток скачать файл
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


def one_txt_file_generator(file_full_name: str) -> bool:  # Генерируем текстовый файл
    source_str = 'abcdefghijklmnopqrstuvwxyz1234567890'
    char_cnt = 2966547  # Количество символов в файле; такое колич-во, например, во всех томах "Война и мир"
    # Создаём файл
    with open(file_full_name, mode='w', encoding='utf8') as file:
        my_string = [choice(source_str) for _ in range(char_cnt)]
        file.write("".join(my_string))
    return file.closed

