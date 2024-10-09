# -*- coding: utf-8 -*-
# m03_thread.py
from datetime import datetime as dt
from threading import Thread
import os
from m00_proc import json_dump, clear_folder, one_txt_file_generator


# --------- Настройки ------------
file_path1 = "01_test_files"  # Тут будут сгенерированные файлы
file_name1 = "f00001.txt"   # Пример имени файла, который будет сгенерирован
file_names1 = []  # Список имён файлов, которые будут сгенерированы
threads1 = []  # Список потоков
file_count = 100
json_name = "du003_threads.json"
du003_dict = {}  # Словарь на выход
# --------------------------------
print('--------- Start ----------')
# Заполняем список имён файлов, которые будут сгенерированы
# А также список Threads
for i in range(file_count):
    name_i = f"f{i + 1:05}.txt"  # Номер файла с лидирующими нулями
    full_name_i = os.path.join(file_path1, name_i)
    file_names1.append(full_name_i)
    thread_i = Thread(target=one_txt_file_generator, args=(full_name_i,))
    threads1.append(thread_i)
    print(f'Create {i} thread', {name_i})
# Первая строка в json-файле, комментарий
du003_dict['Start'] = f'Write to local hard drive by Threads. Number of files={file_count}'
# Очищаем локальную папку-приёмник файлов. Или создаём её, если она ещё не существует.
clear_folder(file_path1)
# Цикл по списку файлов
start_time_whole = dt.now()  # Засекаем ОБЩЕЕ время
for i in threads1:
    # Запускаем один поток из списка потоков.
    print(f'Start {threads1.index(i)} thread')
    i.start()
# Заканчиваем потоки из списка
for i in threads1:
    print(f'Join {threads1.index(i)} thread')
    i.join()
# ---------
# Измеряем ОБЩЕЕ duration — интервал времени
end_time_whole = dt.now()  # Засекаем ОБЩЕЕ время
duration_whole = end_time_whole - start_time_whole
#
du003_dict['Stop'] = (f'-------- The End. {str(duration_whole)} Sec, '
                                     f'file_count={file_count}')
# Записываем в json-файл
file_closed = json_dump(du003_dict, json_name)

print(f'duration_whole = {duration_whole} Sec')
print('-------- The End ---------')

