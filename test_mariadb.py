import os
import re
import sys
import glob
import env/conf
import mysql.connector as mariadb
from datetime import datetime

try:
    conn = mariadb.connect(
        user=conf.maria_db_user,
        password=conf.maria_db_pwd,
        host=conf.maria_db_host,
        database=conf.maria_db_name,
        allow_local_infile='true')

    cur = conn.cursor()

    for file_path in glob.glob(os.path.join('data/', '*.txt')):
        with open(file_path, 'r') as f:
            file_abspath = os.path.abspath(file_path)
            file_name = os.path.basename(file_path)
            table_name = os.path.splitext(file_name)[0]

            # open file and read first row
            first_row = f.readline()

            # populate list with column headers
            header_list = first_row.split('\t')
            create_column = ''
            load_data_col = ''

            # populate variables to create sql that will create the table
            for idx, val in enumerate(header_list):
                if len(load_data_col) == 0:
                    load_data_col = val
                else:
                    load_data_col = load_data_col + ',' + val

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

            # compile table sql
            create_sql = 'create table if not exists ' + table_name +\
                ' (' + create_column + ');'

            cur.execute(create_sql)
            # conn.commit()

            # ingest file to table
            f.seek(0, 0)
            next(f)

            load_data = "LOAD DATA LOCAL INFILE '" + file_abspath +\
                "' INTO TABLE " + table_name +\
                " FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n'\
                IGNORE 1 LINES"
            cur.execute(load_data)
            conn.commit()

except mariadb.Error as e:
    dt = datetime.now().strftime('%Y%m%d %H:%M:%S.%f | ')
    f_path = os.path.expanduser('~/.logs/python-dynamic-data-ingestion.log')
    app_name = os.path.basename(__file__)
    with open(f_path, 'a') as f:
        f.write(dt + app_name + ' | ' + 'MariaDB Error: ' + str(e) + '\n')
    print('MariaDB Error: ' + str(e))
    sys.exit(1)

except Exception as e:
    dt = datetime.now().strftime('%Y%m%d %H:%M:%S.%f | ')
    f_path = os.path.expanduser('~/.logs/python-dynamic-data-ingestion.log')
    app_name = os.path.basename(__file__)
    with open(f_path, 'a') as f:
        f.write(dt + app_name + ' | ' + 'Exception Error: ' + str(e) + '\n')
    print('Exception Error: ' + str(e))
    sys.exit(1)

else:
    dt = datetime.now().strftime('%Y%m%d %H:%M:%S.%f | ')
    f_path = os.path.expanduser('~/.logs/python-dynamic-data-ingestion.log')
    app_name = os.path.basename(__file__)
    with open(f_path, 'a') as f:
        f.write(dt + app_name + ' | ' + 'Successfully Executed...\n')
    print('Successfully Executed...')
    sys.exit(0)
