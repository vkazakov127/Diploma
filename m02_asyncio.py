# -*- coding: utf-8 -*-
# m02_asyncio.py
from random import choice
from datetime import datetime as dt
import asyncio
import os
from m00_proc import json_dump, format_bytes, clear_folder

# --------- Настройки ------------
file_path1 = "01_test_files"
file_name1 = "f00001.txt"
file_names1 = []  # Список имён файлов, которые будут сгенерированы
file_count = 100

json_file_name: str = "du002_asyncio.json"
source_str = 'abcdefghijklmnopqrstuvwxyz1234567890'
char_cnt = 2966547  # Количество символов в файле; такое колич-во, например, во всех томах "Война и мир"
du002_dict = {}  # Словарь на выход


# --------------------------------
async def one_txt_file_generator(file_full_name: str):
    with open(file_full_name, mode='w', encoding='utf8') as file:
        my_string = [choice(source_str) for _ in range(char_cnt)]
        file.write("".join(my_string))
        print(f'{file_full_name} is generated.')
    return file.closed


async def main():
    print('--------- Start ----------')
    # Заполняем список имён файлов, которые будут сгенерированы
    for i in range(file_count):
        name_i = f"f{i + 1:05}.txt"  # Номер файла с лидирующими нулями
        file_names1.append(name_i)
    # Очищаем локальную папку-приёмник файлов. Или создаём её, если она ещё не существует.
    clear_folder(file_path1)
    # Первая строка в json-файле, комментарий
    du002_dict['Start'] = f'Write to local hard drive by "asyncio". Number of files={file_count}'
    # Цикл по списку файлов
    start_time_whole = dt.now()  # Засекаем ОБЩЕЕ время
    tasks1 = []
    for file_name_i in file_names1:
        full_name_i = os.path.join(file_path1, file_name_i)
        # Создаём задачу
        task1 = asyncio.create_task(one_txt_file_generator(full_name_i))
        tasks1.append(task1)
    # А теперь эти задачи выполняем, по списку tasks1
    for t_i in tasks1:
        await t_i
    # ---------
    # Измеряем ОБЩЕЕ duration — интервал времени
    end_time_whole = dt.now()  # Засекаем ОБЩЕЕ время
    duration_whole = end_time_whole - start_time_whole
    #
    du002_dict['Stop'] = (f'-------- The End. {str(duration_whole)} Sec, '
                           f'file_count={file_count}')
    # Записываем в json-файл
    json_file_closed = json_dump(du002_dict, json_file_name)

    print(f'json_file_closed={json_file_closed}')
    print(f'duration_whole = {duration_whole} Sec')
    print('-------- The End ---------')

# Тут происходит асинхронный запуск main()
asyncio.run(main())
