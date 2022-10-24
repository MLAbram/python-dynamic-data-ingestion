import os
import re
import sys
import glob
import xlsxwriter
from datetime import datetime

try:
    for file_path in glob.glob(os.path.join('data/', '*.txt')):
        with open(file_path, 'r') as f:
            file_name = os.path.basename(file_path)
            table_name = os.path.splitext(file_name)[0]

            # open file and read first row
            first_row = f.readline()

            # populate list with column headers
            header_list = first_row.split('\t')
            create_column = ''

            # create sttm
            ws_lnd_header = (['Source', 'DB Name', 'Table Name', 'Column Name',
                              'Data Type'])
            ws_tst_header = (['Source', 'DB Name', 'Table Name', 'Column Name',
                              'Data Type'])
            ws_raw_header = (['Source', 'DB Name', 'Table Name', 'Column Name',
                              'Data Type'])
            ws_lnd_data = []
            ws_tst_data = []
            ws_raw_data = []
            lnd_source = ''
            lnd_db_name = 'lnd_db'
            lnd_table_name = ''
            tst_source = ''
            tst_db_name = 'tst_db'
            tst_table_name = ''
            raw_source = ''
            raw_db_name = 'raw_db'
            raw_table_name = ''

            # loop through column header
            for idx, val in enumerate(header_list):
                if re.search('_num', val):
                    ws_lnd_data.append([file_name, lnd_db_name, table_name,
                                        val, ' numeric(13,2)'])
                    ws_tst_data.append([file_name, lnd_db_name, table_name,
                                        val, ' numeric(13,2)'])
                    ws_raw_data.append([file_name, raw_db_name, table_name,
                                        val, ' numeric(13,2)'])
                elif re.search('_int', val):
                    ws_lnd_data.append([file_name, lnd_db_name, table_name,
                                        val, ' int'])
                    ws_tst_data.append([file_name, lnd_db_name, table_name,
                                        val, ' int'])
                    ws_raw_data.append([file_name, raw_db_name, table_name,
                                        val, ' int'])
                elif re.search('_bint', val):
                    ws_lnd_data.append([file_name, lnd_db_name, table_name,
                                        val, ' bigint'])
                    ws_tst_data.append([file_name, lnd_db_name, table_name,
                                        val, ' bigint'])
                    ws_raw_data.append([file_name, raw_db_name, table_name,
                                        val, ' bigint'])
                elif re.search('_dt', val):
                    ws_lnd_data.append([file_name, lnd_db_name, table_name,
                                        val, ' date'])
                    ws_tst_data.append([file_name, lnd_db_name, table_name,
                                        val, ' date'])
                    ws_raw_data.append([file_name, raw_db_name, table_name,
                                        val, ' date'])
                elif re.search('_ts', val):
                    ws_lnd_data.append([file_name, lnd_db_name, table_name,
                                        val, ' timestamp'])
                    ws_tst_data.append([file_name, lnd_db_name, table_name,
                                        val, ' timestamp'])
                    ws_raw_data.append([file_name, raw_db_name, table_name,
                                        val, ' timestamp'])
                elif re.search('_chr', val):
                    ws_lnd_data.append([file_name, lnd_db_name, table_name,
                                        val, ' chr(1)'])
                    ws_tst_data.append([file_name, lnd_db_name, table_name,
                                        val, ' chr(1)'])
                    ws_raw_data.append([file_name, raw_db_name, table_name,
                                        val, ' chr(1)'])
                elif re.search('_vc', val):
                    ws_lnd_data.append([file_name, lnd_db_name, table_name,
                                        val, ' varchar(50)'])
                    ws_tst_data.append([file_name, lnd_db_name, table_name,
                                        val, ' varchar(50)'])
                    ws_raw_data.append([file_name, raw_db_name, table_name,
                                        val, ' varchar(50)'])
                else:
                    ws_lnd_data.append([file_name, lnd_db_name, table_name,
                                        val, ' text'])
                    ws_tst_data.append([file_name, lnd_db_name, table_name,
                                        val, ' text'])
                    ws_raw_data.append([file_name, raw_db_name, table_name,
                                        val, ' text'])

                # create xlsx workbook, worksheets
                wb = xlsxwriter.Workbook('sttm/' + table_name + '.xlsx')
                ws_lnd = wb.add_worksheet('LND')
                ws_tst = wb.add_worksheet('TST')
                ws_raw = wb.add_worksheet('RAW')

                # create header in lnd
                row = 0
                col = 0
                for i in (ws_lnd_header):
                    ws_lnd.write(row, col, i)
                    col += 1

                # write lnd data
                for row_num, row_data in enumerate(ws_lnd_data):
                    for col_num, col_data in enumerate(row_data):
                        ws_lnd.write(row_num + 1, col_num, col_data)

                    # create header in tst
                    row = 0
                    col = 0
                    for i in (ws_tst_header):
                        ws_tst.write(row, col, i)
                        col += 1

                # write tst data
                for row_num, row_data in enumerate(ws_tst_data):
                    for col_num, col_data in enumerate(row_data):
                        ws_tst.write(row_num + 1, col_num, col_data)

                    # create header in raw
                    row = 0
                    col = 0
                    for i in (ws_raw_header):
                        ws_raw.write(row, col, i)
                        col += 1

                # write raw data
                for row_num, row_data in enumerate(ws_raw_data):
                    for col_num, col_data in enumerate(row_data):
                        ws_raw.write(row_num + 1, col_num, col_data)

                wb.close()

except Exception as e:
    dt = datetime.now().strftime('%Y%m%d %H:%M:%S.%f | ')
    f_path = os.path.expanduser('~/.logs/python-dynamic-data-ingestion.log')
    app_name = os.path.basename(__file__)
    with open(f_path, 'a') as f:
        f.write(dt + app_name + ' | ' + 'Exception Error: ' + e + '\n')
    sys.exit(1)

else:
    dt = datetime.now().strftime('%Y%m%d %H:%M:%S.%f | ')
    f_path = os.path.expanduser('~/.logs/python-dynamic-data-ingestion.log')
    app_name = os.path.basename(__file__)
    with open(f_path, 'a') as f:
        f.write(dt + app_name + ' | ' + 'Successfully Executed...\n')
    print('Successfully Executed...')
    sys.exit(0)
