import os
import re
import sys
import glob
import env/conf
import psycopg2
from datetime import datetime

try:
    conn = psycopg2.connect(
        user=conf.pg_db_user,
        password=conf.pg_db_pwd,
        host=conf.pg_db_host,
        database=conf.pg_db_name)
    cur = conn.cursor()

    for file_path in glob.glob(os.path.join('data/', '*.txt')):
        with open(file_path, 'r') as f:
            file_name = os.path.basename(file_path)
            table_name = os.path.splitext(file_name)[0]

            # open file and read first row
            first_row = f.readline()
            # populate list with column headers
            header_list = first_row.split('\t')
            create_column = ''

            # populate variables to create sql that will create the table
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
                    if len(create_column) == 0:
                        create_column = val + ' char(1) null'
                    else:
                        create_column = create_column + ', ' + val +\
                            ' char(1) null'
                elif re.search('_vc', val):
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
            create_sql = 'create table if not exists ' + conf.pg_db_schema +\
                '.' + table_name + ' (' + create_column + ');'
            cur.execute(create_sql)
            conn.commit()

            # ingest file(s) to table
            f.seek(0, 0)
            next(f)
            cur.copy_from(f, conf.pg_db_schema + '.' + table_name, sep='\t')
            conn.commit()

except psycopg2.Error as e:
    dt = datetime.now().strftime('%Y%m%d %H:%M:%S.%f | ')
    f_path = os.path.expanduser('~/.logs/python-dynamic-data-ingestion.log')
    app_name = os.path.basename(__file__)
    with open(f_path, 'a') as f:
        f.write(dt + app_name + ' | ' + 'Exception Error: ' + str(e) + '\n')
    print('Exception Error: ' + str(e))
    cur.close
    conn.close()
    sys.exit(1)

except Exception as e:
    dt = datetime.now().strftime('%Y%m%d %H:%M:%S.%f | ')
    f_path = os.path.expanduser('~/.logs/python-dynamic-data-ingestion.log')
    app_name = os.path.basename(__file__)
    with open(f_path, 'a') as f:
        f.write(dt + app_name + ' | ' + 'Exception Error: ' + str(e) + '\n')
    print('Exception Error: ' + str(e))
    cur.close
    conn.close()
    sys.exit(1)

else:
    dt = datetime.now().strftime('%Y%m%d %H:%M:%S.%f | ')
    f_path = os.path.expanduser('~/.logs/python-dynamic-data-ingestion.log')
    app_name = os.path.basename(__file__)
    with open(f_path, 'a') as f:
        f.write(dt + app_name + ' | ' + 'Successfully Executed...\n')
    print('Successfully Executed...')
    cur.close
    conn.close()
    sys.exit(0)
