import os
import glob
import re
import sys
from datetime import datetime

delimiter_str = '\t'

try:
    for file_path in glob.glob(os.path.join('data/', '*.txt')):
        with open(file_path, 'r') as f:
            file_name = os.path.basename(file_path)
            table_name = os.path.splitext(file_name)[0]

            # open file and read first row
            first_row = f.readline()

            # remove \n character
            first_row = first_row.strip()

            # populate list with column headers
            # header_list = first_row.split('\t')
            header_list = first_row.split(str(delimiter_str))
            create_column = ''

            # loop through column header
            for idx, val in enumerate(header_list):
                if re.search('_num', val):
                    # create logic to determine length and decimal places
                    if len(create_column) == 0:
                        create_column = val + ' numeric(13,2) null'
                    else:
                        create_column = create_column + ', ' + val +\
                            ' numeric(13,2) null'
                elif re.search('_int', val):
                    if len(create_column) == 0:
                        create_column = val + ' int null'
                    else:
                        create_column = create_column + ', ' + val +\
                            ' int null'
                elif re.search('_bint', val):
                    if len(create_column) == 0:
                        create_column = val + ' bigint null'
                    else:
                        create_column = create_column + ', ' + val +\
                            ' bigint null'
                elif re.search('_dt', val):
                    if len(create_column) == 0:
                        create_column = val + ' date null'
                    else:
                        create_column = create_column + ', ' + val +\
                            ' date null'
                elif re.search('_ts', val):
                    if len(create_column) == 0:
                        create_column = val + ' timestamp null'
                    else:
                        create_column = create_column + ', ' + val +\
                            ' timestamp null'
                elif re.search('_chr', val):
                    # create logic for length
                    if len(create_column) == 0:
                        create_column = val + ' char(1) null'
                    else:
                        create_column = create_column + ', ' + val +\
                            ' char(1) null'
                elif re.search('_vc', val):
                    # create logic to determine length with a max of 255
                    if len(create_column) == 0:
                        create_column = val + ' varchar(50) null'
                    else:
                        create_column = create_column + ', ' + val +\
                            ' varchar(50) null'
                else:
                    if len(create_column) == 0:
                        create_column = val + ' text null'
                    else:
                        create_column = create_column + ', ' + val +\
                            ' text null'

            # create table sql
            create_sql = 'create table if not exists ' + 'db_schema' + '.' +\
                table_name + ' (' + create_column + ');'
            print('-- ' + table_name)
            print(create_sql)
            print('')

            # push file to table
            next(f)

except Exception as e:
    dt = datetime.now().strftime('%Y%m%d %H:%M:%S.%f | ')
    f_log_path = os.path.expanduser('~/.logs/python-dynamic-data-ingestion.log')
    app_name = os.path.basename(__file__)
    with open(f_log_path, 'a') as f_log:
        f_log.write(dt + app_name + ' | ' + 'Exception Error: ' + e + '\n')
    print('Exception Error: ' + e)
    sys.exit(1)

else:
    dt = datetime.now().strftime('%Y%m%d %H:%M:%S.%f | ')
    f_log_path = os.path.expanduser('~/.logs/python-dynamic-data-ingestion.log')
    app_name = os.path.basename(__file__)
    with open(f_log_path, 'a') as f_log:
        f_log.write(dt + app_name + ' | ' + 'Successfully Executed...\n')
    print('Successfully Executed...')
    sys.exit(0)
