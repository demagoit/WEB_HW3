from time import sleep, time
from threading import Thread
import logging
import sys
import shutil
import pathlib
import clean
from multiprocessing import JoinableQueue, Process, Manager, current_process

logging.basicConfig(
    format='pid=%(process)d %(processName)s thread=%(threadName)s %(message)s',
    level=logging.DEBUG,
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger()
# stream_handler = logging.StreamHandler()
# logger.addHandler(stream_handler)
# logger.setLevel(logging.DEBUG)

q_dirs = JoinableQueue()
q_dirs_2_del = JoinableQueue()
q_files = JoinableQueue()
q_result = JoinableQueue()


def dir_crawler(work_dir):
    dirs_2_process = []
    dirs_restricted = []
    try:
        for item in work_dir.iterdir():
            if item.is_dir():
                if item.name.lower() not in clean.EXEPT_DIRS:
                    dirs_2_process.append(item)
                    sub_dirs, restr_dirs = dir_crawler(item)
                    if sub_dirs:
                        dirs_2_process.extend(sub_dirs)
                    if restr_dirs:
                        dirs_restricted.extend(restr_dirs)
    except PermissionError:
        dirs_restricted.append(work_dir)
    return dirs_2_process, dirs_restricted


def dir_crawler_mp(q_dirs: JoinableQueue, q_files: JoinableQueue, q_dirs_2_del: JoinableQueue,):
    '''take start dir from $q_dirs and puts all found dirs to $q_dirs and $q_dirs_2_del,
    all found files puts to $q_files'''

    logger.debug("dir_crawler started...")

    while not q_dirs.empty():
        work_dir = q_dirs.get()
        logger.debug(f"work_dir={work_dir}")
        try:
            for item in work_dir.iterdir():
                if item.is_dir():
                    if item.name.lower() not in clean.EXEPT_DIRS:
                        q_dirs.put(item)
                        q_dirs_2_del.put(item)
                        q_dirs.task_done()
                elif item.is_file():
                    q_files.put(item)

        except PermissionError:
            # dirs_restricted.append(work_dir)
            logger.debug("PermissionError exception...")

    q_dirs.task_done()
    logger.debug("dir_crawler finished")
    sys.exit(0)


def dir_cleaner_mp(q_dirs: JoinableQueue):
    '''takes dir from $q_dirs and removes it'''

    logger.debug("dir_cleaned started...")

    while not q_dirs.empty():
        work_dir = q_dirs.get()
        logger.debug(f"work_dir={work_dir}")
        try:
            work_dir.rmdir()
            q_dirs.task_done()
        except PermissionError:
            # dirs_restricted.append(work_dir)
            logger.debug("PermissionError exception...")
            q_dirs.task_done()
    logger.debug("dir_cleaner finished")
    sys.exit(0)


def copy_file_mp(category_name, path, file, files_found, to_norm=True, to_unpack=False):
    '''
    copy $file to subfolder $category_name in folder @path 
    calls $normalize() if @to_norm = True
    calls $unpack_file if @to_unpack = True
    '''

    dest_dir = path.joinpath(category_name)

    if not dest_dir.exists():
        dest_dir.mkdir()

    if to_unpack:
        clean.unpack_file(dest_dir, file, to_norm=to_norm)
        files_found = clean.update_list(category_name, files_found, file.name)
    else:
        if to_norm:
            dest_file_norm = clean.normalize(file.stem) + file.suffix
        else:
            dest_file_norm = file.name
        files_found = clean.update_list(
            category_name, files_found, dest_file_norm)
        dest_file_norm = dest_dir.joinpath(dest_file_norm)
        shutil.copy2(file, dest_file_norm)

    return files_found


def move_file_mp(category_name, path, file, files_found, to_norm=True, to_unpack=False):
    '''
    move $file to subfolder $category_name in folder @path 
    calls $normalize() if @to_norm = True
    calls $unpack_file if @to_unpack = True
    '''

    dest_dir = path.joinpath(category_name)

    if not dest_dir.exists():
        dest_dir.mkdir()

    if to_unpack:
        clean.unpack_file(dest_dir, file, to_norm=to_norm)
        files_found = clean.update_list(category_name, files_found, file.name)
    else:
        if to_norm:
            dest_file_norm = clean.normalize(file.stem) + file.suffix
        else:
            dest_file_norm = file.name
        files_found = clean.update_list(
            category_name, files_found, dest_file_norm)
        dest_file_norm = dest_dir.joinpath(dest_file_norm)
        shutil.move(file, dest_file_norm, copy_function=shutil.copy2)

    return files_found


def sort_files_mp(q_files: JoinableQueue, q_result: JoinableQueue, target_dir, FILE_TYPES: dict):
    '''
    take file from $q_files, sort and moves files according FILE_TYPES table to folders in $target_dir
    calls $normalize() to all known file types
    unpack all archive files and normalize their content
    '''
    logger.debug("sort_files started...")

    files_found = dict()
    known_types = set()
    unknown_types = set()
    logger.debug(f"sort_files size={q_files.qsize()}")
    while not q_files.empty():
        item = q_files.get()
        logger.debug(f'file={item}')

        if item.is_file():
            file_type = item.suffix.lower()
            unknown_file = True
            for file_category, file_extensions in FILE_TYPES.items():
                if file_type in file_extensions:
                    known_types.add(file_type)
                    if file_category == 'ARCHIVES':
                        pass
                        files_found = move_file_mp(file_category, target_dir, item,
                                                   files_found, to_norm=True, to_unpack=True)
                    else:
                        files_found = move_file_mp(file_category, target_dir,
                                                   item, files_found, to_norm=True)
                    q_files.task_done()
                    unknown_file = False
                    break

            if unknown_file:
                unknown_types.add(file_type)
                files_found = move_file_mp('unknown_types', target_dir,
                                           item, files_found, to_norm=False)
                q_files.task_done()
        else:
            pass

    result = (files_found, known_types, unknown_types)
    q_result.put(result)
    logger.debug('sort_files finished.')
    sys.exit(0)


def mp_manager(path):
    '''initiate and mamage Multiprocess workers'''
    start_time = time()

    logger.debug('MP manager started.')

    with Manager() as mng:
        dir_crawler = Process(
            name='MP-crawler', target=dir_crawler_mp, args=(q_dirs, q_files, q_dirs_2_del))
        q_dirs.put(path)
        dir_crawler.start()

        workers = [dir_crawler]
        for i in range(3):
            file_processor = Process(name=f'MP-worker{i}', target=sort_files_mp, args=(
                q_files, q_result, path, clean.FILE_TYPES))
            file_processor.start()
            workers.append(file_processor)

        [file_processor.join() for file_processor in workers]

        dirs_2_remove = []
        while not q_dirs_2_del.empty():
            dirs_2_remove.append(q_dirs_2_del.get())
            q_dirs_2_del.task_done()

        dirs_2_remove.reverse()
        for item in dirs_2_remove:
            q_dirs_2_del.put(item)
        dir_cleaner = Process(
            name='MP-cleaner', target=dir_cleaner_mp, args=(q_dirs_2_del,))
        dir_cleaner.start()
        dir_cleaner.join()
        q_dirs_2_del.join()

        files_found = dict()
        known_types = set()
        unknown_types = set()

        while not q_result.empty():
            fls, known, unknown = q_result.get()

            files_found.update(fls)
            known_types.update(known)
            unknown_types.update(unknown)
            q_result.task_done()

        q_dirs.join()
        q_files.join()
        q_result.join()

    logger.debug(
        f'MP manager finished after {time()-start_time} seconds')

    return files_found, known_types, unknown_types


def mt_manager(path):
    '''initiate and mamage Multithread workers'''
    start_time = time()

    logger.debug('MT manager started.')

    with Manager() as mng:
        dir_crawler = Thread(
            name='MT-crawler', target=dir_crawler_mp, args=(q_dirs, q_files, q_dirs_2_del))
        q_dirs.put(path)
        dir_crawler.start()
        dir_crawler.join()

        workers = []
        for i in range(3):
            file_processor = Thread(name=f'MT-worker{i}', target=sort_files_mp, args=(
                q_files, q_result, path, clean.FILE_TYPES))
            file_processor.start()
            workers.append(file_processor)

        [file_processor.join() for file_processor in workers]

        dirs_2_remove = []
        while not q_dirs_2_del.empty():
            dirs_2_remove.append(q_dirs_2_del.get())
            q_dirs_2_del.task_done()

        dirs_2_remove.reverse()
        for item in dirs_2_remove:
            q_dirs_2_del.put(item)
        dir_cleaner = Thread(
            name='MT-cleaner', target=dir_cleaner_mp, args=(q_dirs_2_del,))
        dir_cleaner.start()
        dir_cleaner.join()
        q_dirs_2_del.join()

        files_found = dict()
        known_types = set()
        unknown_types = set()

        while not q_result.empty():
            fls, known, unknown = q_result.get()

            files_found.update(fls)
            known_types.update(known)
            unknown_types.update(unknown)
            q_result.task_done()

        q_dirs.join()
        q_files.join()
        q_result.join()

    logger.debug(f'MT manager finished after {time()-start_time} seconds')

    return files_found, known_types, unknown_types


def rec_manager(path):
    '''calls recursive function from clean.py'''
    start_time = time()

    logger.debug('Recursion manager started.')

    files_found, known_types, unknown_types = clean.sort_files(path, path)

    logger.debug(f'Recursion manager finished after {time()-start_time} seconds')

    return files_found, known_types, unknown_types


def main():

    if not len(sys.argv) > 1:
        print(f'Usage: {sys.argv[0]} <path to dir>')
        sys.exit(1)
    else:
        path = pathlib.Path(sys.argv[1])

    if not path.is_dir():
        print(f'Path {path} is not a dir or not exists.')
        return

    # dirs_2_go, dirs_restricted = dir_crawler(path)
    # print(len(dirs_2_go), dirs_2_go[:20])
    # print('---')
    # print(len(dirs_restricted), dirs_restricted[:20])

    print("Select folder processing method:")
    print("1. Multiprocess")
    print("2. Multithread")
    print("3. Recursion")
    choise = input("make your choise: ")

    if choise == '1':
        files_found, known_types, unknown_types = mp_manager(path)
    elif choise == '2':
        files_found, known_types, unknown_types = mt_manager(path)
    elif choise == '3':
        files_found, known_types, unknown_types = rec_manager(path)
    else:
        print("Wrong number.")

    # for category, files in files_found.items():
    #     print(f'Category {category} includes:\n{files}')
    # print(
    #     f'Source folder and subfolders has following known file types:\n{known_types or "_"}')
    # print(
    #     f'Source folder and subfolders has following unknown file types:\n{unknown_types or "_"}')


if __name__ == '__main__':
    main()
