import sys
import shutil
import pathlib


EXEPT_DIRS = ('archives', 'video', 'audio',
              'documents', 'images', 'unknown_types')
FILE_TYPES = {
'ARCHIVES' : ('.zip', '.gz', '.tar'),
'VIDEO' : ('.avi', '.mp4', '.mov', '.mkv'),
'AUDIO' : ('.mp3', '.ogg', '.wav', '.amr'),
'DOCUMENTS' : ('.doc', '.docx', '.txt', '.pdf', '.xlsx', '.pptx'),
'IMAGES' : ('.jpeg', '.png', '.jpg', '.svg')
}
TRANSLIT = {
    ord('а'):'a', ord('б'):'b', ord('в'):'v', ord('г'):'h', ord('ґ'):'g', ord('д'):'d', ord('е'):'e',
    ord('є'):'ie', #Ye
    ord('ж'):'zh', ord('з'):'z', ord('и'):'y', ord('і'):'i', ord('ї'):'i', #Yi
    ord('й'):'i', #Y
    ord('к'):'k', ord('л'):'l', ord('м'):'m', ord('н'):'n', ord('о'):'o', ord('п'):'p', ord('р'):'r',
    ord('с'):'s', ord('т'):'t', ord('у'):'u', ord('ф'):'f', ord('х'):'kh', ord('ц'):'ts',
    ord('ч'):'ch', ord('ш'):'sh', ord('щ'):'shch', ord('ю'):'iu', #Yu
    ord('я'):'ia', #Ya
    ord('А'):'A', ord('Б'):'B', ord('В'):'V', ord('Г'):'H', ord('Ґ'):'G', ord('Д'):'D', ord('Е'):'E',
    ord('Є'):'IE', #Ye
    ord('Ж'):'ZH', ord('З'):'Z', ord('И'):'Y', ord('І'):'I', ord('Ї'):'I', #Yi
    ord('Й'):'I', #Y
    ord('К'):'K', ord('Л'):'L', ord('М'):'M', ord('Н'):'N', ord('О'):'O', ord('П'):'P', ord('Р'):'R',
    ord('С'):'S', ord('Т'):'T', ord('У'):'U', ord('Ф'):'F', ord('Х'):'KH', ord('Ц'):'TS',
    ord('Ч'):'CH', ord('Ш'):'SH', ord('Щ'):'SHCH', ord('Ю'):'IU', #Yu
    ord('Я'):'IA', #Ya
}


def normalize(str):
    '''
    takes $str and normalize it according TRANSLIT table
    '''
    
    translated_str = str.translate(TRANSLIT)
    translated_lst = [i if i.isalnum() else '_' for i in translated_str]
    translated_str = ''.join(translated_lst)
    return translated_str


def normalize_dir(dest_dir):
    '''
    normalize all files and subfolders in $dest_dir
    '''

    for item in dest_dir.iterdir():
        if item.is_dir():
            sub_dir = item.parent.joinpath(normalize(item.name))

            if sub_dir != item:
                item.rename(sub_dir)

            normalize_dir(sub_dir)
        else:
            dest_file_norm = item.parent.joinpath(
                normalize(item.stem) + item.suffix)
            if dest_file_norm != item:
                item.rename(dest_file_norm)
    return


def update_list(category_name, files_dict, file):
    '''
    add $file to category $category_name in @files_dict dictionary
    '''
    
    store_list = files_dict.get(category_name, list())
    store_list.append(file)
    files_dict[category_name] = store_list
    return files_dict


def unpack_file(path, file, to_norm=True):
    '''
    unpack $file to subfolder @file.name in folder $path and delete $file afterwards
    calls $normalize_dir() if @to_norm = True
    @file.name is normalized anyway
    '''
    dest_dir = path.joinpath(normalize(file.stem))

    if not dest_dir.exists():
        dest_dir.mkdir()

    shutil.unpack_archive(file, dest_dir)
    file.unlink()

    if to_norm:
        normalize_dir(dest_dir)

    return


def move_file(category_name, path, file, files_found, to_norm = True, to_unpack = False):
    '''
    moves $file to subfolder $category_name in folder @path 
    calls $normalize() if @to_norm = True
    calls $unpack_file if @to_unpack = True
    '''
    
    dest_dir = path.joinpath(category_name)
    
    if not dest_dir.exists():
        dest_dir.mkdir()
    
    if to_unpack:
        unpack_file(dest_dir, file, to_norm=to_norm)
        files_found = update_list(category_name, files_found, file.name)
    else:
        if to_norm:
            dest_file_norm = normalize(file.stem) + file.suffix
        else:    
            dest_file_norm = file.name
        files_found = update_list(category_name, files_found, dest_file_norm)
        dest_file_norm = dest_dir.joinpath(dest_file_norm)
        shutil.move(file, dest_file_norm, copy_function=shutil.copy2)
    
    return files_found


def copy_file(category_name, path, file, files_found, to_norm=True, to_unpack=False):
    '''
    copy $file to subfolder $category_name in folder @path 
    calls $normalize() if @to_norm = True
    calls $unpack_file if @to_unpack = True
    '''

    dest_dir = path.joinpath(category_name)

    if not dest_dir.exists():
        dest_dir.mkdir()

    if to_unpack:
        unpack_file(dest_dir, file, to_norm=to_norm)
        files_found = update_list(category_name, files_found, file.name)
    else:
        if to_norm:
            dest_file_norm = normalize(file.stem) + file.suffix
        else:
            dest_file_norm = file.name
        files_found = update_list(category_name, files_found, dest_file_norm)
        dest_file_norm = dest_dir.joinpath(dest_file_norm)
        shutil.copy2(file, dest_file_norm)

    return files_found


def sort_files(work_dir, target_dir):
    '''
    walk through $work_dir, sort and moves files in all subfolders according FILE_TYPES table to folders in $target_dir
    calls $normalize() to all known file types
    unpack all archive files and normalize their content
    '''

    files_found = dict()
    known_types = set()
    unknown_types = set()
    for item in work_dir.iterdir():
        if item.is_dir():
            if item.name.lower() not in EXEPT_DIRS:
                sub_files, sub_known_types, sub_unknown_types = sort_files(item, target_dir)
                files_found.update(sub_files)
                known_types.union(sub_known_types)
                unknown_types.union(sub_unknown_types)
                item.rmdir()
        elif item.is_file():
            file_type = item.suffix.lower()
            unknown_file = True
            for file_category, file_extensions in FILE_TYPES.items():
                if file_type in file_extensions:
                    known_types.add(file_type)
                    if file_category == 'ARCHIVES':
                        files_found = move_file(file_category, target_dir, item,
                                  files_found, to_norm=True, to_unpack=True)
                    else:
                        files_found = move_file(file_category, target_dir,
                                  item, files_found, to_norm=True)
                    unknown_file = False
                    break
            if unknown_file:
                unknown_types.add(file_type)
                files_found = move_file('unknown_types', target_dir,
                          item, files_found, to_norm=False)
        else:
            print(f"Item {item} is neither file nor dir. Skipping...")

    return files_found, known_types, unknown_types


def main():
    # print(sys.argv[1])
    if not len(sys.argv)>1:
        print(f'Usage: {sys.argv[0]} <path to dir>')
        sys.exit(1)
    path = pathlib.Path(sys.argv[1])
    if not path.is_dir():
        print(f'Path {path} is not a dir or not exists.')
        return
    file_list, known_types, unknown_types = sort_files(path, path)
    
    for category, files in file_list.items():
        print(f'Category {category} includes:\n{files}')
    print(f'Source folder and subfolders has following known file types:\n{known_types or "_"}')
    print(f'Source folder and subfolders has following unknown file types:\n{unknown_types or "_"}')

if __name__ == '__main__':
    main()