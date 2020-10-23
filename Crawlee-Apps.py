#!/Users/agupt323/AppData/Local/Programs/Python/Python36 python
import codecs
import feedparser
import json
import os
import pandas as pd
import pyodbc
import re
import requests
#import smtplib
import sys
import urllib

from pathlib                import Path
from datetime               import *
from os                     import listdir
from os.path                import basename
from pprint                 import pprint as pp
#from email.mime.application import MIMEApplication
#from email.mime.multipart   import MIMEMultipart
#from email.mime.text        import MIMEText
#from email.utils            import COMMASPACE, formatdate

global __HOME_DIR__
global __FOLDER_LIST_FILE__
global __TD_TABLE_LIST_FILE__
global __TD_ATTRIBUTE_LIST_FILE__
global __CS_TABLE_LIST_FILE__
global __CS_ATTRIBUTE_LIST_FILE__
global __NAC_OUTPUT_FILE__
global __MERCHANT_ERS_FILE__

global __TD_TABLE_LIST__
global __TD_ATTRIBUTE_LIST__
global __CS_TABLE_LIST__
global __CS_ATTRIBUTE_LIST__
global __KEY_WORD_LIST__
global __TD_CONNECTSTRING__
global __CS_CONNECTSTRING__

#Setting up the Home Folder. It can be passed as an configuration argument
__HOME_DIR__ = 'C:\\Code\\Crawlee'

#Setting up all file locations.
__FOLDER_LIST_FILE__       = __HOME_DIR__ + '\\Data\\folder_list.txt'
__TD_TABLE_LIST_FILE__     = __HOME_DIR__ + '\\Data\\TD_Tables.csv'
__TD_ATTRIBUTE_LIST_FILE__ = __HOME_DIR__ + '\\Data\\TD_Attributes.csv'
__CS_TABLE_LIST_FILE__     = __HOME_DIR__ + '\\Data\\CS_Tables.csv'
__CS_ATTRIBUTE_LIST_FILE__ = __HOME_DIR__ + '\\Data\\CS_Attributes.csv'
__STC_OUTPUT_FILE__        = __HOME_DIR__ + '\\Data\\STC_Results.txt'
__STC_LOG__                = __HOME_DIR__ + '\\Data\\STC_Log.txt'
__TUC_OUTPUT_FILE__        = __HOME_DIR__ + '\\Data\\TUC_Results.txt'
__TUC_LOG__                = __HOME_DIR__ + '\\Data\\TUC_Log.txt'
__NAC_OUTPUT_FILE__        = __HOME_DIR__ + '\\Data\\NAC_Results.txt'
__NAC_LOG__                = __HOME_DIR__ + '\\Data\\NAC_Log.txt'
__ERS_OUTPUT_FILE__        = __HOME_DIR__ + '\\Data\\ERS_Results.txt'
__ERS_LOG__                = __HOME_DIR__ + '\\Data\\ERS_Log.txt'
__MERCHANT_ERS_FILE__      = __HOME_DIR__ + '\\Data\\Merchant_ERS_Summary.txt'

#Setting up the connection strings
__TD_CONNECTSTRING__ = 'DRIVER=SQLServer;UID=Ashish;PWD=****;AUTHENTICATION=LDAP;DATABASE=Merchants;DBCNAME=edw.skynet.com'
__CS_CONNECTSTRING__ = 'https://test.skynet.com:8000/cdms/storages?all_feeds=true'

#Initializing global variables
__KEY_WORD_LIST__     = []
__TD_TABLE_LIST__     = []
__TD_ATTRIBUTE_LIST__ = []
__CS_TABLE_LIST__     = []
__CS_ATTRIBUTE_LIST__ = []

#Calling Main
if __name__ == '__name__':
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6])


def main(arg_file_or_folder_flag, arg_subject_list, arg_subfolder_search_flag, arg_file_type_list, arg_search_name_flag, arg_search_token):
    """CRAWLEE: A generic capability for performing Searches for a string token in a single or multiple selected
       files or folders and returns their location in the file(s).

    Args:
        File Or Folder Flag     - FILE/FOLDER
        Subject List            - List of Files or Folders to search through
        Include Subfolder Flag  - TRUE/FALSE
        File Type List          - List of File Types to search through or * for all file types. ( Applicable for Folder Search Only )
        Search Name Flag        - TRUE/FALSE ( Whether to search in file contents or names only. Applicable for Folder Search Only )
        Search Token            - String Token for searching

    Returns:
        List of following variables: File Name, Search Token, Result Counter, Line Number, Position, File Line Content
    """
    #Calling Library Function for Token Search as Default
    final_results = Cr_search_token (arg_file_or_folder_flag, arg_subject_list, arg_subfolder_search_flag, arg_file_type_list, arg_search_name_flag, arg_search_token)

    #Processing through all result lines
    [print('FILE:', line[1], ', TOKEN:', line[3], ', RESULT NO:', line[0], ', LINE:', line[4], ', POS:', line[5], ', LINE TEXT: "', line[2], '"') for line in final_results]


#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def Cr_search_token (arg_file_or_folder_flag, arg_subject_list, arg_subfolder_search_flag, arg_file_type_list, arg_search_name_flag, arg_search_token, arg_output_type):
    """Searches a string token in a single or multiple selected files or folders and returns their location in the file(s).

    Args:
        File Or Folder Flag         - FILE/FOLDER
        Subject List                - List of Files or Folders to search through
        Include Subfolder Flag      - TRUE/FALSE
        File Type List              - List of File Types to search through or * for all file types. ( Applicable for Folder Search Only )
        Search Name Flag            - TRUE/FALSE ( Whether to search in file contents or names only. Applicable for Folder Search Only )
        Search Token                - String Token for searching
        Output Type                 - REPORT / CSV
    Returns:
        List of following variables: File Name, Search Token, Result Counter, Line Number, Position, File Line Content
    """
    #Variables Declaration
    search_counter = 0
    ret_final_results = []
    result_set = []

    #Configuring the Arguments
    arg_output_type = arg_output_type.upper()

    #Opening the files for log and output writing
    w_results_file = codecs.open(__STC_OUTPUT_FILE__, 'w', "utf-8")
    w_log_file  = codecs.open(__STC_LOG__, 'w', "utf-8")

    start_TS = datetime.now()

    write_line = 'Started at: ' + str(start_TS) + '.'
    w_log_file.write(write_line)

    if arg_output_type == 'REPORT': print(write_line)

    # -----------------------------------------------------
    #Setting up the file list to process through
    # -----------------------------------------------------
    if arg_file_or_folder_flag == 'FOLDER':
        file_list = uf_generate_file_list(arg_subject_list, arg_subfolder_search_flag, arg_file_type_list)
    elif arg_file_or_folder_flag == 'FILE':
        file_list = arg_subject_list
    else:
        raise

    # Searching through each file
    file_count = len(file_list)
    file_number = 0

    # Displaying Processing Log Banner
    write_line = '\n' + ('#' * 80) + '\nProcessing Log\n' + ('#' * 80)
    w_log_file.write(write_line)
    if arg_output_type == 'REPORT':
        print(write_line)

    # -----------------------------------------------------
    # Processing through the final file list
    # -----------------------------------------------------
    for file_name in file_list:

        # Displaying file processing progress....
        file_number += 1

        write_line = '\nProcessing File (' + str(file_number).strip() + '/' + str(file_count).strip() + '):' + file_name + '.\n'
        w_log_file.write(write_line)

        if arg_output_type == 'REPORT':
            print(write_line)

        # ----------------------------------
        # Logic for searching in file name
        # ----------------------------------
        if arg_search_name_flag:
            col_index_list = uf_search_token_in_text(file_name, arg_search_token)
            # ---------------------------------------
            # Adding the result for each found position
            # ---- ----------------------------------
            for col_index in col_index_list:
                search_counter += 1
                #Adding the result
                ret_final_results.append((search_counter, file_name, file_name.replace('\n', ''), arg_search_token, 1, col_index))
        elif not arg_search_name_flag:
            # Adding the results
            ret_final_results += uf_search_token_in_file(file_name, arg_search_token)
        else:
            raise

        #Writing into log file
        write_line = 'Done.'
        w_log_file.write(write_line)

        #Displayig the Output
        if arg_output_type == 'REPORT':
            print(write_line)


    write_line = ('#' * 80) + '\n' + 'Results\n' + ('#' * 80) + '\n'
    if arg_output_type == 'REPORT':
        w_results_file.write(write_line)
        print(write_line)
    elif arg_output_type == 'JSON':
        write_line = '[\n'
        w_results_file.write(write_line)
        print(write_line.replace('\n', ''))
    else:
        pass

    for i, line in enumerate(ret_final_results):

        dict_row =  [("FILE", line[1]), ("TOKEN", line[3]), ("RESULT_NO", line[0]), ("LINE", line[4]), ("POS", line[5]), ("LINE_TEXT", line[2])]
        result_set.append(dict(dict_row))

        if arg_output_type.upper() == 'REPORT':
            write_line = 'FILE:' + line[1] + ', TOKEN:' + line[3] + ', RESULT NO:' + str(line[0]) + ', LINE:' + str(line[4]) + ', POS:' + str(line[5]) + ', LINE TEXT: "' + line[2] + '"\n'
        elif arg_output_type.upper()== 'CSV':
            write_line = line[1].strip() +',' + line[3].strip() + ',' + str(line[0]).strip() + ',' + str(line[4]).strip() + ',' + str(line[5]).strip() + '\n'
        elif arg_output_type.upper() == 'JSON':
            if i == len(ret_final_results) - 1:
                line_end = ''
            else:
                line_end = ','
            write_line = '{"FILE": "' + line[1] + '", "TOKEN": "' + line[3] + '", "RESULTNO": "' + str(line[0]) + '", "LINE": ' + str(line[4]) + ', "POS": ' + str(line[5]) + ', "LINETEXT": "' + line[2] + '"}' + line_end + '\n'
        else:
            write_line = ''

        w_results_file.write(write_line)
        print(write_line.replace('\n', ''))

    #Writing into log
    end_TS = datetime.now()
    write_line = '\n' + ('#' * 80) + '\n\nStarted at: ' + str(start_TS) + '. Finished at: ' + str() + '. Time Taken: ' + str(end_TS - start_TS)
    w_log_file.write(write_line)

    if arg_output_type == 'REPORT':
        print(write_line)
    elif arg_output_type == 'JSON':
        write_line = ']'
        w_results_file.write(write_line)
        print(write_line)
    else:
        pass

    w_log_file.close()
    w_results_file.close()

    ret_json = json.dumps(result_set)
    return ret_json



#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def Cr_table_usage (arg_codebase, arg_file_or_folder_flag, arg_subject_list, arg_subfolder_search_flag, arg_file_type_list,  arg_schema, arg_output_type):
    """Searches a table' and its attributes in a single or multiple selected files or folders and returns their usage in the file(s).

    Args:
        Code Base                   - TD/HIVE/SQL
        File Or Folder Flag         - FILE/FOLDER
        Subject List                - List of Files or Folders to search through
        Include Subfolder Flag      - TRUE/FALSE
        File Type List              - List of File Types to search through or * for all file types. ( Applicable for Folder Search Only )
        Schema Name                 - Default Schema to use if schema is not named in the script
        Output Type                 - Report / CSV

    Returns:
        List of following variables: File Name, Search Token, Result Counter, Line Number, Position, File Line Content
    """
    #Variables Declaration
    file_list = []
    ret_results_set = []
    statement_list = []
    prev_file_name = ''
    KEY_WORD_FILE = __HOME_DIR__ + '\\Data\\Key_Words.csv'

    #Configuring the Arguments
    arg_output_type = arg_output_type.upper()

    #Opening the files for log and output writing
    w_results_file = codecs.open(__TUC_OUTPUT_FILE__, 'w', "utf-8")
    w_log_file  = codecs.open(__TUC_LOG__, 'w', "utf-8")

    start_TS = datetime.now()

    write_line = 'Started at: ' + str(start_TS) + '.'
    w_log_file.write(write_line)

    if arg_output_type == 'REPORT': print(write_line)

    #Reading SQL Keywords from the SQL Word CSV File
    with open(KEY_WORD_FILE, 'rt') as f:
        for line in f:
            __KEY_WORD_LIST__.append(line.replace('\n',''))

    #uf_refresh_catalog('TD')
    if arg_codebase == 'TD':
        df_tables = pd.read_csv(__TD_TABLE_LIST_FILE__)
    elif arg_codebase == 'CS':
        df_tables = pd.read_csv(__CS_TABLE_LIST_FILE__)
    else:
        pass

    # -----------------------------------------------------
    #If search is to be performed at folder level
    # -----------------------------------------------------
    if arg_file_or_folder_flag == 'FOLDER':
        file_list = uf_generate_file_list(arg_subject_list, arg_subfolder_search_flag, arg_file_type_list)

    # -----------------------------------------------------
    # If search is to be performed at file level
    # -----------------------------------------------------
    elif arg_file_or_folder_flag == 'FILE':
        file_list = arg_subject_list

    else:
        raise

    w_list = []
    found_tables = []

    #Searching through each file
    file_count = len(file_list)
    file_number = 0

    #Displaying Processing Log Banner
    write_line = '\n' + ('#' * 80) + '\nProcessing Log\n' + ('#' * 80)
    w_log_file.write(write_line)
    if arg_output_type == 'REPORT':
        print(write_line)

    #Processing each file
    for file_name in file_list:

        #Displaying file processing progress....
        file_number += 1

        write_line = '\nProcessing File (' + str(file_number).strip() + '/' + str(file_count).strip() + '): ' + file_name + '.\n'
        w_log_file.write(write_line)

        if arg_output_type == 'REPORT':
            print(write_line.replace('\n', ''))

        #Getting all statements from the script file
        statement_list = uf_get_file_statements(arg_codebase, file_name)

        #Processing each file for finding tables
        for statement in statement_list:

            table_words = set()
            column_word_list = []

            #Word Cleansing: Stripping all characters except alphanumerics, '.' and '_'. Then generating word list.
            w = ''.join(c if c.isalpha() or c == '_' or c == '.' else ' ' for c in statement.upper() )
            final_word_list = []

            word_list = w.split()

            for word in word_list:
                if word in __KEY_WORD_LIST__:
                    pass
                else:
                    final_word_list.append(word)

            #Examining each cleansed word from statement
            word_number = -1
            word_count = len(final_word_list)

            #Evaluating each word for table name
            for word in final_word_list:

                #preparing the word for search
                word = word.strip()
                word_number += 1

                #Searching for DB Name & Table Name in the current word if present
                if word.find('.') >= 0:
                    if arg_codebase == 'TD':
                        w = word.split('.')
                        DBName = w[0]
                        TName = w[1]
                    elif arg_codebase == 'CS':
                        w = word.split('.')
                        DBName = arg_schema.lower()
                        TName = w[1].lower()
                    else:
                        pass
                else:
                    if arg_codebase == 'TD':
                        if word[0] == '_':
                            DBName = 'UDW_DATA'
                            TName = word
                        else:
                            DBName = arg_schema
                            TName = word
                    elif arg_codebase == 'CS':
                        DBName = arg_schema.lower()
                        TName = word.lower()
                    else:
                        pass

                #Checking whether the word is a table name or not?
                found_rows = df_tables[(df_tables['DatabaseName'] == DBName) & (df_tables['TableName'] == TName)]

                #if it is a table
                if len(found_rows) > 0:
                    tp = (file_name, DBName, TName)
                    found_tables.append(tp)
                    table_words.add(word)
            for w in list(table_words):
                if w not in table_words:
                    column_word_list.append(w)
            #print(column_word_list)
            # Evaluating each word for table name

        #Writing into log file
        write_line = 'Done.'
        w_log_file.write(write_line)

        #Displayig the Output
        if arg_output_type == 'REPORT':
            print(write_line)

    sorted_list = list(set(found_tables))
    sorted_list.sort()

    #Displaying and logging the final results

    #Displaying and logging the header if applicable on output mode
    if arg_output_type == 'REPORT':
        # Preparing the Results Banner
        write_line = ('#' * 80) + '\n' + 'Results\n' + ('#' * 80) + '\n'
        w_results_file.write(write_line)
        print(write_line)
    elif arg_output_type == 'JSON':
        write_line = '[\n'
        w_results_file.write(write_line)
        print(write_line.replace('\n', ''))
    else:
        pass

    # Displaying and logging the result data
    for i, row in enumerate(sorted_list):
        file_name = row[0]
        abs_file_name = file_name[file_name.rfind('\\') + 1:]
        DBName = row[1]
        TName = row[2]

        dict_row =  [("File", abs_file_name), ("DBName", DBName), ("TableName", TName)]
        ret_results_set.append(dict(dict_row))

        if arg_output_type.upper() == 'REPORT':

            if file_name != prev_file_name:
                write_line = '\n' + (' ' * 4) + ('-' * 75) + '\n' + '     Script: ' + file_name + '\n' + (' ' * 4) + ('-' * 75) + '\n' + (' ' * 80) + '\n' + (' ' * 5) + 'DB Name' + (' ' * 13) + 'Table/View Name\n' + (' ' * 5) + ('_' * 18) + '  ' + ('_' * 30)

                w_results_file.write(write_line)
                print(write_line)

            write_line = '\n' + (' ' * 5) + DBName + (' ' * (20-len(DBName))) + TName

        elif arg_output_type.upper()== 'CSV':
            write_line = abs_file_name.strip() + ',' + DBName.strip() + ',' + TName.strip() + '\n'
        elif arg_output_type.upper() == 'JSON':

            #Setting up line's last char as ',' if not the last line
            if i == len(sorted_list) - 1:
                line_end = ''
            else:
                line_end = ','

            write_line = '{"FILE": "' + abs_file_name.strip() + '", "DBName": "' + DBName.strip() + '", "TableName": "' + TName.strip() + '"}' + line_end + '\n'
        else:
            write_line = ''

        w_results_file.write(write_line)
        print(write_line.replace('\n', ''))

        prev_file_name = file_name

    #Writing into log
    end_TS = datetime.now()
    write_line = '\n' + ('#' * 80) + '\n\nStarted at: ' + str(start_TS) + '. Finished at: ' + str() + '. Time Taken: ' + str(end_TS - start_TS)
    w_log_file.write(write_line)

    if arg_output_type == 'REPORT':
        print(write_line)
    elif arg_output_type == 'JSON':
        write_line = ']'
        w_results_file.write(write_line)
        print(write_line)
    else:
        pass

    w_log_file.close()
    w_results_file.close()
    ret_json = json.dumps(ret_results_set)
    return ret_json

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
def Cr_CS_table_usage (arg_codebase, arg_file_or_folder_flag, arg_subject_list, arg_subfolder_search_flag, arg_file_type_list,  arg_schema, arg_output_type):
    arg_codebase = arg_codebase.upper()
    uf_refresh_catalog(arg_codebase)

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def uf_generate_file_list(arg_subject_list, arg_subfolder_search_flag, arg_file_type_list):

    #Variables
    file_list = []

    # Setting up the folder list file for generating recursive list of folders
    with open(__FOLDER_LIST_FILE__, 'w') as file_handle:
        [file_handle.write(folder + '\n') for folder in arg_subject_list]

    # Processing all folders present in argument folder list
    [uf_find_all_subfolders(folder) for folder in arg_subject_list if arg_subfolder_search_flag]

    # Retrieving the final list of all folders to be searched into
    file_handle = open(__FOLDER_LIST_FILE__, 'rt')
    final_folder_list = file_handle.readlines()
    file_handle.close()

    # Going through each folder from the final list for generating final file list
    for folder in final_folder_list:
        file_path = folder
        if file_path.find('\n') > 0 and file_path[len(file_path) - 1:]:
            file_path = file_path[0:len(file_path) - 1]

        all_files = os.listdir(file_path)
        [file_list.append((file_path + '\\' + file)) for file in all_files if file[file.find('.') + 1:] in arg_file_type_list or arg_file_type_list[0] == '*']
    return file_list

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def uf_find_all_subfolders(arg_folder):
    #Removing new line character from the argument folder if present
    if arg_folder.find('\n') > 0 and arg_folder[len(arg_folder)-1:]:
        arg_folder = arg_folder[0:len(arg_folder)-1]

    #generating the file list from argument folder
    file_list = os.listdir(arg_folder)

    w_arg_folder = Path(__FOLDER_LIST_FILE__)

    #Processing all files present in the argument folder
    for file in file_list:
        #Generating the full file name
        file_name = arg_folder + '\\' + file

        #If the current file is a folder
        if os.path.isdir(file_name):

            #Deciding on the file mode based on whether it is present or not
            if w_arg_folder.exists():
                file_mode = 'a'
            else:
                file_mode = 'w'

            #Adding the current file/folder in the Folder List File
            w_file_handle = open(__FOLDER_LIST_FILE__, file_mode)
            w_file_handle.write(file_name + '\n')
            w_file_handle.close()

            #Find the subfolders for the current file/folder
            uf_find_all_subfolders(file_name)
        else:
            pass
    return

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def uf_search_token_in_text(search_subject, search_token):
    """Searches for a argument string token in the argument text string and returns the list of all positions where the token was found.

    Args:
        search_subject: Argument Text String where the search is to be performed.

        search_token: Argument String Token which is to be searched.

    Returns:
        Returns a list of integer position values where token was found in the text string.
    """
    #Variables Initialization
    search_index = 1
    result_set = []

    #Performing the string search till the string is exhausted.
    while True:
        search_index = search_subject.upper().find(search_token.upper(), search_index)
        if search_index == -1:
            break

        result_set.append(search_index)
        search_index += len(search_token) + 1

    return result_set

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def uf_search_token_in_file(search_subject, search_token):
    # Variables
    line_num = 0
    search_counter = 0
    col_index_list = []
    line_result = ()
    file_result = []

    # Read the file
    try:
        r_file_handle = open(search_subject, 'rt')
        all_lines = r_file_handle.readlines()
        r_file_handle.close()

        # Variables
        line_num = 0
        search_counter = 0
        col_index_list = []
        line_result = ()
        file_result = []

        # Read all lines for searching
        for line in all_lines:
            line_num += 1
            if line.strip() is None or line.strip() == '':
                continue

            line = line.replace('\n', '')
            line = line.replace('\t', '    ')

            col_index_list = uf_search_token_in_text(line, search_token)

            for col_index in col_index_list:
                search_counter += 1
                line_result = (search_counter, search_subject, line.replace('\n', ''), search_token, line_num, col_index)
                file_result.append(line_result)
    except:
        pass
    return file_result

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def uf_get_file_statements(arg_codebase, arg_file):
    all_statements = []
    line_count = 0
    statement = ''
    try:

        #Reading the file content
        w_file_handle = open(arg_file, 'rt')
        all_lines = w_file_handle.readlines()
        w_file_handle.close()

        #Processing the file content line by line
        for line in all_lines:
            line_count += 1

            #Removing new line character from current line
            line = line.replace('\n', '')

            l = line.strip()
            #if the current line is an empty line or comments then skip the current line
            if l == '' or l == '\n' or l[0:2] == '--':
                continue

            #Add the current line to statement
            statement += line + '\n'

            #Extracting the last word from current line , based on spaces
            line_words = l.split()
            last_word = line_words[len(line_words) - 1]

            #If the last word is : then mark it as a statement and add it to the current statement and list of statements and start with the new statement
            if last_word == ';' or l[len(l)-1] ==';':
                all_statements.append(statement)
                statement = ''
    except:
        pass
    return all_statements

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def uf_get_TD_dataset(arg_sql):

    data_set = []

    # Creating Connection Object
    conn = pyodbc.connect(__TD_CONNECTSTRING__)
    cursor = conn.cursor()

    cursor.execute(arg_sql)
    while True:
        row = cursor.fetchone()
        if not row:
            break
        data_set.append(tuple(row))
    return data_set

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def uf_refresh_catalog(arg_codebase):
    if arg_codebase == 'TD':
        # Retrieving Tables List
        table_sql = ('Select Trim(Upper(DatabaseName)), Trim(Upper(TableKind)), Trim(Upper(TableName)) '
                     'from dbc.tables '
                     'where DatabaseName In ( \'UDW\', \'UDW_BVWS\', \'UDW_DATA\', \'IMPAC_BVWS\', \'IMPAC\', \'IMPAC_DATA\' ) And '
                     'TableKind in (\'T\', \'V\', \'F\') and '
                     'TableName not like \'h_%\' and TableName not like \'z0_%\' and TableName not like \'z1_%\' and TableName not like \'z2_%\' and TableName not like \'z3_%\' and TableName not like \'r_%\' and TableName not like \'d_%\' and TableName not like \'gtt_%\' order by 1,2,3')
        table_list = uf_get_TD_dataset(table_sql)

        # Saving the tables list in a file
        file_handle = open(__TD_TABLE_LIST_FILE__, 'w')
        file_handle.write('DatabaseName,TableType,TableName\n')
        for line in table_list:
            try:
                l = line[0] + ',' + line[1] + ',' + line[2] + '\n'
                file_handle.write(l)
                l = ''
            except Exception:
                print('Error', l)
                raise
        file_handle.close()

        # Retrieving Attribute List
        attribute_sql = ('Select Trim(Upper(A.DatabaseName)), Trim(Upper(A.TableName)), Trim(Upper(A.ColumnName)) from dbc.columns A Inner Join dbc.tables B On A.DatabaseName = B.DatabaseName and A.TableName = B.TableName where A.DatabaseName In ( \'UDW\', \'UDW_BVWS\', \'UDW_DATA\', \'IMPAC_BVWS\', \'IMPAC\', \'IMPAC_DATA\' ) And B.TableKind in (\'T\', \'V\', \'F\') and B.TableName not like \'h_%\' and B.TableName not like \'z0_%\' and B.TableName not like \'z1_%\' and B.TableName not like \'z2_%\' and B.TableName not like \'z3_%\' and B.TableName not like \'r_%\' and B.TableName not like \'d_%\' and B.TableName not like \'gtt_%\' order by 1, 2, 3')
        attribute_list = uf_get_TD_dataset(attribute_sql)

        # Saving the attribute list in a file
        file_handle = open(__TD_ATTRIBUTE_LIST_FILE__, 'w')
        file_handle.write('DatabaseName,TableName,ColumnName\n')

        for line in attribute_list:
            try:
                l = line[0] + ',' + line[1] + ',' + line[2] + '\n'
                file_handle.write(l)
                l = ''
            except Exception:
                print('Error', l)
                raise
        file_handle.close()
        ret_list = __TD_TABLE_LIST__

    elif arg_codebase == 'CS':
        print("Inside fetching func")

        #Getting CS Tables
        response = requests.get(__CS_CONNECTSTRING__)

        f = open(__CS_TABLE_LIST_FILE__, "w")
        f.write('DatabaseName,TableType,TableName\n')

        # Print the status code of the response.
        if (response.ok):
            result_set = json.loads(response.text)

        table_list = result_set['response'][0]["result_list"]

        for item in table_list:
            if (item["table_name"]):
                f.write('cstonedb3,T,' + item["table_name"] + '\n')
                #__CS_TABLE_LIST__.add(item["table_name"])

        f.close

        # Getting CS Attributes
        response = requests.get(__CS_CONNECTSTRING__)

        f = open(__CS_ATTRIBUTE_LIST_FILE__, "w")
        f.write('DatabaseName,TableType,TableName \n')

        # Print the status code of the response.
        if (response.ok):
            result_set = json.loads(response.text)

        table_list = result_set['response'][0]["result_list"]

        for item in table_list:
            if (item["table_name"]):
                f.write('cstonedb3,T,' + item["table_name"] + '\n')
                # __CS_TABLE_LIST__.add(item["table_name"])

        f.close

    else:
        pass

    ret_list = []

    return ret_list

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def uf_load_catalog(arg_codebase):
    __TD_TABLE_LIST__ = []
    __TD_ATTRIBUTE_LIST__ = []
    __CS_TABLE_LIST__ = []
    Table_File = ''
    Attribute_File = ''

    if arg_codebase == 'TD':
       Table_File     = __TD_TABLE_LIST_FILE__
       Attribute_File = __TD_ATTRIBUTE_LIST_FILE__
    elif arg_codebase == 'CS':
        Table_File = __CS_TABLE_LIST_FILE__
        Attribute_File = __CS_ATTRIBUTE_LIST_FILE__
    else:
        pass

    # Retrieving Tables List
    file_handle = open(Table_File, 'rt')
    all_lines = file_handle.readlines()
    file_handle.close()

    for line in all_lines:
        l = str(line).replace('\n','')
        if arg_codebase == 'TD':
            __TD_TABLE_LIST__.append(tuple(l.split(',')))
        elif arg_codebase == 'CS':
            __CS_TABLE_LIST__.append(tuple(l.split(',')))

    # Retrieving Attribute List
    file_handle = open(Attribute_File, 'rt')
    all_lines = file_handle.readlines()
    file_handle.close()

    for line in all_lines:
        l = str(line).replace('\n','')
        if arg_codebase == 'TD':
            __TD_ATTRIBUTE_LIST__.append(tuple(l.split(',')))
        elif arg_codebase == 'CS':
            __CS_ATTRIBUTE_LIST__.append(tuple(l.split(',')))

    if arg_codebase == 'TD':
        ret_list = __TD_TABLE_LIST__
    elif arg_codebase == 'CS':
        ret_list = __CS_TABLE_LIST__
    else:
        ret_list = []

    return ret_list

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def uf_get_file_first_words(arg_file):
    first_words = []
    try:
        w_file_handle = open(arg_file, 'rt')
        all_lines = w_file_handle.readlines()
        w_file_handle.close()

        for line in all_lines:
            if line.strip() != '':
                first_word = (line.strip().split())[0]
                first_words.append(first_word)
    except:
        pass
    return first_words

#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

def validate_args(arg_folder, arg_subfolder_flag, arg_file_type, arg_search_context, arg_string):

    # Variables
    error_codes = {}
    error_def = []
    error_key = "0"
    error_msg = str()

    ERROR_CODE_FILE = __HOME_DIR__ + '\\Crawlee_error_codes.csv'

    #Reading Error Codes File and creating a dictionary for codes and messages
    r_file_handle = open(ERROR_CODE_FILE, mode='rt', encoding='utf-8')
    all_lines = r_file_handle.readlines()
    r_file_handle.close()

    for line in all_lines:
        error_def = line.split(',')
        error_codes[error_def[0]] = error_def[1]

    if arg_folder is None or arg_folder.strip() == '':
        error_key = '-1'
    elif type(arg_folder) != 'Dict':
        error_key = '-2'
    else:
        pass

    if error_key == '0':
        ret_code = '0'
        ret_msg = ''
    else:
        ret_code = error_key
        ret_msg = error_codes[error_key]

    return ret_code, ret_msg
