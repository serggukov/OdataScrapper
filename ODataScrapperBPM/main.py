import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
import yaml
import glob
import logging
from datetime import date, datetime
import pyodbc
from time import sleep


def logs(message, logtype='info'):
    if logtype == 'info':
        logging.info(message)
    elif logtype == 'error':
        logging.error(message)
    elif logtype == 'critical':
        logging.critical(message)

    if verbose:
        now = str(datetime.now())[:-6]
        print(now, message)


def execute_query(select=False, **kwargs):
    ms_sql_db_host = kwargs.get('ms_sql_db_host', '')
    ms_sql_db = kwargs.get('ms_sql_db', '')
    ms_sql_db_user = kwargs.get('ms_sql_db_user', '')
    ms_sql_db_pass = kwargs.get('ms_sql_db_pass', '')

    query = kwargs.get('query', '')

    res = []

    if not ms_sql_db or not ms_sql_db_host or not ms_sql_db_user or not ms_sql_db_pass or not query:
        logs('Not enough parameters for query', 'error')
        return
    connstring = r'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + ms_sql_db_host \
                       + ';DATABASE=' + ms_sql_db \
                       + ';UID=' + ms_sql_db_user \
                       + ';PWD=' + ms_sql_db_pass

    try:
        cnxn = pyodbc.connect(connstring)
        cursor = cnxn.cursor()
    except Exception:
        logs('Error opening ODBC', 'critical')
        raise ConnectionError
    try:
        cursor.execute(query)
        if select:
            for row in cursor.fetchall():
                res.append(row)
        cnxn.commit()
    except Exception:
        logs(f'Error in query {query}', 'error')
    finally:
        cnxn.close()

    if select:
        return res


def get_types():
    odataToSQLTypes = dict()
    odataToSQLTypes['Edm.Int64'] = 'BIGINT'
    odataToSQLTypes['Edm.Binary'] = 'BINARY'
    odataToSQLTypes['Edm.Boolean'] = 'BIT'
    odataToSQLTypes['Edm.Int32'] = 'INTEGER'
    odataToSQLTypes['Edm.String'] = 'nvarchar(MAX)'
    odataToSQLTypes['Edm.Date'] = 'Date'
    odataToSQLTypes['Edm.Decimal'] = 'FLOAT'
    odataToSQLTypes['Edm.Double'] = 'FLOAT'
    odataToSQLTypes['Edm.Binary'] = 'VARBINARY'
    odataToSQLTypes['Edm.Single'] = 'REAL'
    odataToSQLTypes['Edm.Int16'] = 'SMALLINT'
    odataToSQLTypes['Edm.TimeOfDay'] = 'TIME'
    odataToSQLTypes['Edm.DateTimeOffset'] = 'TIMESTAMP'
    odataToSQLTypes['Edm.Byte'] = 'TINYINT'
    odataToSQLTypes['Edm.SByte3'] = 'TINYINT'
    return odataToSQLTypes


def get_create_table_query(name, fields):
    odataToSQLTypes = get_types()

    querytext = f"IF EXISTS \n(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{name}') \nBEGIN\n"
    querytext += f'DROP TABLE [dbo].[{name}]'
    querytext += '\nEND;'

    querytext += f"CREATE TABLE [dbo].[{name}]("
    for field in fields:
        fieldtype = odataToSQLTypes.get(fields[field], 'nvarchar(MAX)')
        querytext += '\n' + f"[{field}] {fieldtype} NULL,"
    querytext += ") ON [PRIMARY];"
    querytext = querytext.replace(',)', ')')

    return querytext


def get_insert_table_queries(name, records):
    querytext = ''
    if not records:
        return querytext
    querytext = f'INSERT INTO [dbo].[{name}] ('
    mask = records[0]
    for field in mask:
        querytext += '\n' + f'[{field}],'
    querytext += ') VALUES '
    querytext = querytext.replace(',)', ')')
    for record in records:
        rec = tuple()
        for field in mask:
            _rec = record[field]
            if _rec is None:
                _rec = ""
            _rec = str(_rec)
            if _rec == 'StandardODATA.Undefined':
                _rec = ""
            if "'" in _rec:
                _rec = _rec.replace("'", '"')
            rec += (_rec,)
        querytext += str(rec)
        querytext += ','
    return querytext[:-1]


def get_json(xml):
    meta = dict()
    res = dict()
    for field in xml:
        if 'content' in field.tag:
            for subfield in field[0]:
                fieldname = subfield.tag
                pos = 1 + str(fieldname).find('}')
                fieldname = fieldname[pos:]
                if subfield.attrib:
                    for attr in subfield.attrib:
                        if 'type' in attr:
                            meta[fieldname] = subfield.attrib[attr]
                        else:
                            meta[fieldname] = 'Edm.String'
                            break
                else:
                    meta[fieldname] = 'Edm.String'
                res[fieldname] = subfield.text
    return res, meta


def readnext(url, session, first, name, request_timeout=60):
    global global_config
    if url:
        logs(f'   reading next for {name}, url ={url}')
        nexturl = ''
        results = list()
        metadata = dict()
        if not first:
            sent_url = url + '&$top=1000'
        else:
            sent_url = url

        response = session.get(sent_url, timeout=request_timeout)

        if response.status_code == 400:
            response = session.get(url, timeout=request_timeout)

        if response.status_code == 200:
            xml_text = response.text
            root = ET.fromstring(xml_text)
            for element in root:
                if 'entry' in element.tag:
                    if first:
                        entry, metadata = get_json(element)
                    else:
                        entry, _ = get_json(element)
                    results.append(entry)
                elif 'link' in element.tag:
                    rel = element.attrib.get('rel', '')
                    url = element.attrib.get('href', '')
                    if rel == 'next':
                        nexturl = url
            if metadata:
                query = get_create_table_query(name, metadata)
                execute_query(**global_config, query=query)
            if results:
                query = get_insert_table_queries(name, results)
                if query:
                    logs('    sending to SQL')
                    execute_query(**global_config, query=query)
            if nexturl:
                return nexturl, False
        else:
            if response.status_code == 404:
                logs(f'Error {response.status_code}', 'error')
                return None, False
            else:
                logs(f'Error {response.status_code}', 'error')
                return url, first
    return None, False


def run(filename):
    global verbose
    global global_config
    logs(f'Starting with {filename}', 'info')
    with open(filename, encoding='UTF-8') as json_file:
        settings = yaml.load(json_file, Loader=yaml.FullLoader)
    if not settings:
        logs(f'Error loading settings from {filename}', 'error')
        return

    global_config = settings['global_config']
    base_url = global_config['base_url']
    request_timeout = global_config.get('request_timeout', 60)

    log_mode = global_config['log_mode']
    if log_mode == "verbose":
        verbose = True

    # create session for HTTP-requests
    api_login = global_config['api_login']
    api_password = global_config['api_pwd']

    auth = HTTPBasicAuth(api_login, api_password)
    session = requests.Session()
    session.auth = auth

    tables = settings['tables']
    logs(f'found tables: {len(tables)}', 'info')
    for table in tables:
        logs(f'Start with {table}')
        tabledict = tables[table]
        url_request = base_url + tabledict['data_request']
        first = True
        while url_request:
            try:
                url_request, first = readnext(url_request, session, first, table, request_timeout)
            except Exception as E:
                logs(f'!!! error sending {url_request}', 'error')
                sleep(60)

        logs(f'Done {table}')
    session.close()
    logs(f'Done: {filename}', 'info')


verbose = False
today = str(date.today())
logging.basicConfig(filename=f'{today}.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
mask = '*.yaml'
global_config = ''
for filename in glob.glob(mask):
    run(filename)
