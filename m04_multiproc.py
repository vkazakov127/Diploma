# -*- coding: utf-8 -*-
# m04_multiproc.py
from datetime import datetime as dt
import os
import multiprocessing as mp
from m00_proc import json_dump, format_bytes, clear_folder, one_txt_file_generator


if __name__ == '__main__':
    # --------- Настройки ------------
    file_path1 = "01_test_files"  # Тут будут сгенерированные файлы
    file_name1 = "f00001.txt"   # Пример имени файла, который будет сгенерирован
    file_names1 = []  # Список имён файлов, которые будут сгенерированы
    file_count = 100
    file_size_whole = 0  # Общий размер всех записанных файлов, в Байтах
    json_name = "du004_multiproc.json"

    du004_dict = {}  # Словарь на выход: результаты измерений
    # --------------------------------
    print('--------- Start ----------')
    # Заполняем список имён файлов, которые будут сгенерированы
    for i in range(file_count):
        name_i = f"f{i + 1:05}.txt"  # Номер файла с лидирующими нулями
        full_name_i = os.path.join(file_path1, name_i)
        file_names1.append(full_name_i)
    # Очищаем локальную папку-приёмник файлов. Или создаём её, если она ещё не существует.
    clear_folder(file_path1)
    # Первая строка в json-файле, комментарий
    du004_dict['Start'] = f'Write to local hard drive by Multiprocessing. Number of files={file_count}'
    # Цикл по списку файлов
    start_time_whole = dt.now()  # Засекаем ОБЩЕЕ время
    print('---- Prepare process pool')
    with mp.Pool(processes=50) as pool:
        # Тут идут процессы, 50 штук
        pool.map(one_txt_file_generator, file_names1)
    print('---- End process pool')
    # Это конец пула процессов.
    # Измеряем общее duration — интервал времени
    end_time_whole = dt.now()  # Засекаем ОБЩЕЕ время
    duration_whole = end_time_whole - start_time_whole
    #
    file_size_whole = sum(d.stat().st_size for d in os.scandir(file_path1) if d.is_file())
    du004_dict['Stop'] = (f'-------- The End. {str(duration_whole)} Sec, '
                                         f'{file_size_whole} Bytes = {format_bytes(file_size_whole)}')
    # Записываем в json-файл
    file_closed = json_dump(du004_dict, json_name)

    print(f'duration_whole = {duration_whole} Sec, {file_size_whole} Bytes = {format_bytes(file_size_whole)}')
    print('-------- The End ---------')
