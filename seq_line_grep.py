import yaml
import re
import enum
import os
import pandas as pd
import sys


class Grepper:
    line_id = -1

    def __init__(self, 
    table_name, 
    key_substring, 
    field_names, 
    regexp_pattern):
        self.table_name = table_name
        self.key_substring = key_substring
        self.field_names = field_names
        self.re_pattern = re.compile(regexp_pattern)
        self.records = []

    def add_record(self, line):
        # check if substring is included in the line
        # this is expected to be False most of the times.
        if self.key_substring in line:
            # get match
            # this is expected to be true most of the times.
            match = self.re_pattern.match(line)
            # in case match is None, .group() will error
            try:
                record = list(match.groups())
            except AttributeError as e:
                print(e)
                print(f"""Mismatch Warning!\nline={line}\nkey_substring={self.key_substring}\nre_pattern={self.re_pattern}""")
                return False
            else:
                # if groups() is get successfully, increment the line count
                self.__class__.line_id += 1
                # append line count to record as line_id
                self.records.append([self.__class__.line_id] + record)
                return True
        else:
            return False


class SettingsDocumentIndex(enum.IntEnum):
    FILE_SETTINGS = 0
    COMMON_PATTERNS = 1
    UNIQUE_PATTERNS = 2


class Settings:

    def __init__(self, fp_settings):
        with open(fp_settings, 'r') as f:
            self.settings = list(yaml.safe_load_all(f))

    # file search options
    def file_path_filter_regexp(self) -> str:
        return self.settings[SettingsDocumentIndex.FILE_SETTINGS]['file_path_filter_regexp']

    def search_recursively(self) -> bool:
        return self.settings[SettingsDocumentIndex.FILE_SETTINGS]['search_recursively']

    def sort_by_date_modified(self) -> bool:
        return self.settings[SettingsDocumentIndex.FILE_SETTINGS]['sort_by_date_modified']

    def code_page_read(self) -> str:
        return self.settings[SettingsDocumentIndex.FILE_SETTINGS]['code_page_read']

    def code_page_write(self) -> str:
        return self.settings[SettingsDocumentIndex.FILE_SETTINGS]['code_page_write']

    def output_file_extension(self) -> str:
        return self.settings[SettingsDocumentIndex.FILE_SETTINGS]['output_file_extension']

    def column_separator_character_integer(self) -> str:
        return self.settings[SettingsDocumentIndex.FILE_SETTINGS]['column_separator_character_integer']

    # regular expression objects
    def get_grepper_list(self):
        grepper_lst = []
        for unique_pattern in self.settings[SettingsDocumentIndex.UNIQUE_PATTERNS]:
            cls_regexp = Grepper(
                table_name=unique_pattern['table_name'],
                key_substring=unique_pattern['key_substring'],
                field_names=['line_id']+self.settings[SettingsDocumentIndex.COMMON_PATTERNS]['field_names'] + unique_pattern['field_names'],
                regexp_pattern=self.settings[SettingsDocumentIndex.COMMON_PATTERNS]['regexp_pattern'] + unique_pattern['regexp_pattern'] + '\n')
            grepper_lst.append(cls_regexp)
        return grepper_lst


def get_fp_list(dir_root, settings):
    """
    loop through files recursively or without recursion.
    filter the file path list by path regular expression
    sort by file modified date
    return the file path list
    """
    # populate the file list
    fp_lst = []

    def populate_file_list_recusively():        
        for root, dirs, files in os.walk(dir_root):
            for file in files:
                fp_lst.append(os.path.join(root, file))
 
    def populate_file_list_without_recursion():
       for file in os.listdir(dir_root):
            fp = os.path.join(dir_root, file)
            if os.path.isfile(fp):
                fp_lst.append(fp)
    
    if settings.search_recursively():
        populate_file_list_recusively()
    else:
        populate_file_list_without_recursion()
    
    # filter by regular expression pattern
    fp_pattern = re.compile(settings.file_path_filter_regexp())
    fp_lst = list(filter(lambda x: fp_pattern.match(x), fp_lst))

    # sort by file modified date
    if settings.sort_by_date_modified():
        fp_lst.sort(key=lambda x: os.path.getmtime(x))

    return fp_lst


def get_paths():
    print(f'len(sys.argv)={len(sys.argv)}')

    if len(sys.argv) == 4:
        fp_settings = sys.argv[1]
        dir_src = sys.argv[2]
        dir_out = sys.argv[3]
    else:
        fp_settings = input('settings.yaml: ').replace('"', '')
        dir_src = input('Data Folder: ').replace('"', '')
        dir_out = input('Out Folder: ').replace('"', '')

    return fp_settings, dir_src, dir_out

def main():

    # get paths from the user
    fp_settings, dir_src, dir_out = get_paths()

    
    # initialize
    settings = Settings(fp_settings)
    grepper_lst = settings.get_grepper_list()
    fp_lst = get_fp_list(dir_src, settings)
    
    # populate data
    for fp in fp_lst:
        with open(fp, 'r', encoding=settings.code_page_read()) as f:
            for line in f.readlines():
                for grepper in grepper_lst:
                    if grepper.add_record(line):
                        break

    for grepper in grepper_lst:
        df = pd.DataFrame(data=grepper.records, columns=grepper.field_names)
        df.index.name='row_id'
        print(df)

        fp_out = os.path.join(dir_out, grepper.table_name + settings.output_file_extension())

        df.to_csv(fp_out, sep=chr(settings.column_separator_character_integer()), encoding=settings.code_page_write())

if __name__ == '__main__':
    main()
