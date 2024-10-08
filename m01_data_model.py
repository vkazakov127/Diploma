# -*- coding: utf-8 -*-
# m01_data_model.py
from random import choice
from datetime import datetime as dt
import os
from m00_proc import json_dump, format_bytes, clear_folder


def one_txt_file_generator(file_full_name: str):
    with open(file_full_name, mode='w', encoding='utf8') as file:
        my_string = [choice(source_str) for _ in range(char_cnt)]
        file.write("".join(my_string))
    return file.closed


# --------- Настройки ------------
file_path1 = "01_test_files"  # Тут будут сгенерированные файлы
file_name1 = "f00001.txt"  # Пример имени файла, который будет сгенерирован
file_names1 = []  # Список имён файлов, которые будут сгенерированы
file_count = 100
file_size_whole = 0  # Общий размер всех записанных файлов, в Байтах
json_name = "du001_data_model.json"
duration_dict = {}  # Словарь на выход: результаты измерений
source_str = 'abcdefghijklmnopqrstuvwxyz1234567890'
char_cnt = 2966547  # Количество символов в файле; такое колич-во, например, во всех томах "Война и мир"
# --------------------------------
print('--------- Start ----------')
# Заполняем список имён файлов, которые будут сгенерированы
for i in range(file_count):
    # Имя файла будет такое: типа "f00001.txt"
    name_i = f"f{i + 1:05}.txt"  # Номер файла с лидирующими нулями
    file_names1.append(name_i)
# Первая строка в json-файле, комментарий
duration_dict['Start'] = f'Write to local hard drive. Number of files={file_count}'
# Очищаем локальную папку-приёмник файлов. Или создаём её, если она ещё не существует.
clear_folder(file_path1)
# Цикл по списку файлов
start_time_whole = dt.now()  # Засекаем ОБЩЕЕ время
for file_name_i in file_names1:
    # Засекаем время на один файл
    start_time_i = dt.now()
    full_name_i = os.path.join(file_path1, file_name_i)
    # Генерируем текстовый файл
    file_closed = one_txt_file_generator(full_name_i)
    # Вычисляем размер файла file_name_i
    file_size_i = os.path.getsize(full_name_i)
    file_size_whole += file_size_i
    # Засекаем время на один файл
    end_time_i = dt.now()
    duration_i = end_time_i - start_time_i
    # Записываем в словарь duration_dict
    # {file_name: [duration, file_size, file_path, file_name]}
    duration_dict[file_name_i] = [str(duration_i), file_size_i, file_path1, file_name_i]
    print(f'{file_name_i}, {str(duration_i)} Sec, {file_size_i} Bytes = {format_bytes(file_size_i)}, '
          f'общий размер = {format_bytes(file_size_whole)}')
# ---------
# Измеряем ОБЩЕЕ duration — интервал времени
end_time_whole = dt.now()  # Засекаем ОБЩЕЕ время
duration_whole = end_time_whole - start_time_whole
#
duration_dict['Stop'] = (f'-------- The End. {str(duration_whole)} Sec, '
                         f'{file_size_whole} Bytes = {format_bytes(file_size_whole)}')
# Записываем в json-файл
file_closed = json_dump(duration_dict, json_name)

print(f'file_closed={file_closed}')
print(f'duration_whole = {duration_whole} Sec, {file_size_whole} Bytes = {format_bytes(file_size_whole)}')
print('-------- The End ---------')
