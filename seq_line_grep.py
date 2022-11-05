import datetime
from operator import index
import yaml
import re
import enum
import os
import pandas as pd
import argparse
from tqdm import tqdm

log_lst = []

def current_time():
    return datetime.datetime.now().strftime('%H:%M:%S')

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
                err_evt = f"""{current_time()} Pattern'{self.re_pattern}' failed with keyword '{self.key_substring}' on line'{line}'"""
                log_lst.append(err_evt)
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
            self.doc_lst = list(yaml.safe_load_all(f))

    def get_file_settings(self, key_name):
        return self.doc_lst[SettingsDocumentIndex.FILE_SETTINGS][key_name]

    # regular expression objects
    def get_grepper_list(self):
        grepper_lst = []
        for unique_pattern in self.doc_lst[SettingsDocumentIndex.UNIQUE_PATTERNS]:
            cls_regexp = Grepper(
                table_name=unique_pattern['table_name'],
                key_substring=unique_pattern['key_substring'],
                field_names=['line_id']+self.doc_lst[SettingsDocumentIndex.COMMON_PATTERNS]['field_names'] + unique_pattern['field_names'],
                regexp_pattern=self.doc_lst[SettingsDocumentIndex.COMMON_PATTERNS]['regexp_pattern'] + unique_pattern['regexp_pattern'] + '\n')
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
    
    if settings.get_file_settings('search_recursively'):
        populate_file_list_recusively()
    else:
        populate_file_list_without_recursion()
    
    # filter by regular expression pattern
    fp_pattern = re.compile(settings.get_file_settings('file_path_filter_regexp'))
    fp_lst = list(filter(lambda x: fp_pattern.match(x), fp_lst))

    # sort by file modified date
    if settings.get_file_settings('sort_by_date_modified'):
        fp_lst.sort(key=lambda x: os.path.getmtime(x))

    return fp_lst

def concatenate_tables(fp_settings, dir_src, dir_dst):
    # initialize
    settings = Settings(fp_settings)
    sep = chr(int(settings.get_file_settings('column_separator_character_integer')))
    encoding = settings.get_file_settings('code_page_write')
    extension = settings.get_file_settings('output_file_extension')
    df_lst = []
    keys = []
    for root, dirs, files in os.walk(dir_src):
        for file in files:
            fp = os.path.join(root, file)
            df = pd.read_csv(fp, sep=sep, index_col=1, encoding=encoding)
            df_lst.append(df)
            keys.append(os.path.splitext(file)[0])
    df_all = pd.concat(df_lst, keys=keys)
    fn_out = '~'.join(keys) + extension
    fp_out = os.path.join(dir_dst, fn_out)
    df_all.to_csv(fp_out, sep=sep, encoding=encoding)

def create_tables(fp_settings, dir_src, dir_out):

    # initialize
    settings = Settings(fp_settings)
    grepper_lst = settings.get_grepper_list()
    fp_lst = get_fp_list(dir_src, settings)

    # loop through files and show the progress bar with tqdm
    print(f'{current_time()} start reading files...')
    for fp in tqdm(fp_lst):
        with open(fp, 'r', encoding=settings.get_file_settings('code_page_read')) as f:
            while True:
                try:
                    line = f.readline()
                # ignore the decoding errors.
                except UnicodeDecodeError as e:
                    err_msg = f'{current_time()} Decode Error on {fp}. Details: {e}'
                    log_lst.append(err_msg)
                    continue
                else:
                    # If line length is 0, get out of wihle loop.
                    # In case of empty line, line length is 1 because of '\n'
                    # line length 0 only occurs at the end of the file.
                    if len(line) == 0:
                        break
                    # populate grepper
                    for grepper in grepper_lst:
                        if grepper.add_record(line):
                            break

    print(f'{current_time()} saving files...')
    for grepper in tqdm(grepper_lst):
        df = pd.DataFrame(data=grepper.records, columns=grepper.field_names)
        df.index.name='row_id'
        fp_out = os.path.join(dir_out, grepper.table_name + settings.get_file_settings('output_file_extension'))
        df.to_csv(fp_out, 
        sep=chr(settings.get_file_settings('column_separator_character_integer')), 
        encoding=settings.get_file_settings('code_page_write'))
        log_lst.append(f'{current_time()} {fp_out} was created.')

    print(f'{current_time()} completed table creation.')

    fp_log = os.path.join(dir_out, 'log.yaml')
    with open(fp_log, 'w', encoding='utf-8') as f:
        yaml.safe_dump(log_lst, f, default_flow_style=False)

    print(f'{current_time()} log file "{fp_log}" was created.')

def main():
    parser = argparse.ArgumentParser(description='Grep and concatenate tables')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-g', '--grep', action='store_true', help='Grep groups from text files and create tables')
    group.add_argument('-c', '--concatenate', action='store_true', help='Concatenate the grepped tables')
    parser.add_argument('-y', '--yamlfile', metavar='file_yaml', required=True, help='YAML file that stores Grep settings')
    parser.add_argument('-i', '--inputdir', metavar='dir_src', required=True, help='Input folder with data files')
    parser.add_argument('-o', '--outputdir', metavar='dir_dst', required=True, help='Empty folder to output files')
    args = parser.parse_args()    

    if args.grep:
        create_tables(args.yamlfile, args.inputdir, args.outputdir)
    elif args.concatenate:
        concatenate_tables(args.yamlfile, args.inputdir, args.outputdir)
    else:
        print('Select either -g or -c')

if __name__ == '__main__':
    main()
