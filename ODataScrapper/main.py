import requests
from requests.auth import HTTPBasicAuth
import xml.etree.ElementTree as ET
import json
import yaml
import glob
import logging
import traceback
from datetime import date, timedelta, datetime
import pyodbc
from math import ceil
from time import sleep


def logs(message, logtype='info'):
    if logtype == 'info':
        logging.info(message)
    elif logtype == 'error':
        logging.error(message)
        logging.error(traceback.format_exc())
    elif logtype == 'critical':
        logging.critical(message)
        logging.error(traceback.format_exc())
    if verbose:
        now = str(datetime.now())[:-6]
        print(now, message)


# ===================== Working with dates START =====================
def str_to_date(str_param):
    year = int(str_param[:4])
    month = int(str_param[5:7])
    day = int(str_param[8:10])
    return date(year, month, day)


def generate_dates(start, finish, inc):
    startdate = str_to_date(start)
    finishdate = str_to_date(finish)

    days = int(inc[:-1])

    if 'y' in inc:
        days = 365 * days
    elif 'm' in inc:
        days = 30 * days
    elif 'w' in inc:
        days = 7 * days

    array_of_dates = []
    if finishdate < startdate:
        return array_of_dates

    d1 = startdate
    while d1 < finishdate:
        d2 = min(finishdate, d1 + timedelta(days=days))
        array_of_dates.append((str(d1)+'T00:00:00', str(d2)+'T23:59:59'))
        d1 = d2 + timedelta(days=1)
    return array_of_dates
# =====================  Working with dates END=====================


# ===================== Working with SQL START =====================
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
    except Exception as E:
        logs(f'Error {E} opening ODBC', 'critical')
        raise ConnectionError
    try:
        cursor.execute(query)
        if select:
            for row in cursor.fetchall():
                res.append(row)
        cnxn.commit()
    except Exception as E:
        logs(f'Error {E} in query {query}', 'error')
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


def get_create_table_query(name, orginalname, metadata, indexes=None):
    queries = []

    fields = metadata.get(orginalname, None)
    if not fields:
        logs(f'Can not find table {orginalname} in meta', 'error')
        return ''

    odataToSQLTypes = get_types()

    querytext = f"IF NOT EXISTS \n(SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{name}') \nBEGIN\n"
    querytext += f"CREATE TABLE [dbo].[{name}]("
    for field in fields:
        if 'Collection(StandardODATA.' in fields[field]:
            new_name = fields[field].replace('Collection(StandardODATA.', '')
            new_name = new_name.replace('_RowType)', '')
            subquery = get_create_table_query(name + '_' + field, new_name, metadata, indexes)
            queries.append(subquery)
            continue
        fieldtype = odataToSQLTypes.get(fields[field], 'nvarchar(MAX)')
        querytext += '\n' + f"[{field}] {fieldtype} NULL,"
    querytext += ") ON [PRIMARY];"
    if indexes is not None and len(indexes) != 0:
        querytext += f"\nCREATE CLUSTERED INDEX [{name}] ON [dbo].[{name}]("
        for field in indexes:
            querytext += f"[{field}] ASC,"
        querytext += ")"
    querytext = querytext.replace(',)', ')')
    querytext += '\nEND;'

    for sub in queries:
        querytext += '\n' + sub
    return querytext


def checktable(name, orginalname, metadata, **kwargs):
    query = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{name}'"
    cols_rows = execute_query(select=True, query=query, **kwargs)
    cols_sql = set()
    for each in cols_rows:
        cols_sql.add(each[0])
    cols_meta = set()
    for each in metadata[orginalname]:
        cols_meta.add(each)
    add_fields = cols_meta - cols_sql
    if len(add_fields) != 0:
        query = f'DROP TABLE [dbo].[{name}]'
        execute_query(query=query, **kwargs)
        query = get_create_table_query(name, orginalname, metadata)
        execute_query(query=query, **kwargs)
        return False
    return True


def deleterows(**kwargs):
    table = kwargs.get('table_name', None)
    if not table:
        return ''
    if kwargs.get('all', True):
        return f'TRUNCATE TABLE [dbo].[{table}];'
    else:
        date_field = kwargs.get('date_field', None)
        date_from = kwargs.get('date_from', str(date.today()))
        date_to = kwargs.get('date_to', str(date.today()))
        if not (date_field and date_from and date_to):
            return
        else:
            return f"DELETE FROM [dbo].[{table}] WHERE [{date_field}] BETWEEN '{date_from}T00:00:00' and '{date_to}T23:59:59';"


def get_insert_table_queries(name, json_text):
    portion = 1000
    records = json_text.get('value', None)
    queries = []
    if not records:
        return queries
    querytext = f'INSERT INTO [dbo].[{name}] ('
    mask = records[0]
    for field in mask:
        if isinstance(mask[field], list):
            continue
        querytext += '\n' + f'[{field}],'
    querytext += ') VALUES '
    querytext = querytext.replace(',)', ')')

    params = []
    for record in records:
        rec = tuple()
        for field in mask:
            _rec = record[field]
            if isinstance(_rec, list):
                if len(_rec) == 0:
                    continue
                sub = dict()
                sub['value'] = _rec
                queries += (get_insert_table_queries(name + '_' + field, sub))
                continue
            if _rec is None:
                _rec = ""
            _rec = str(_rec)
            if _rec == 'StandardODATA.Undefined':
                _rec = ""
            if "'" in _rec:
                _rec = _rec.replace("'", '"')
            rec += (_rec,)
        params.append(rec)

    for querynum in range(ceil(len(params) / portion)):
        newquery = querytext
        first = True
        for i in range(portion * querynum, portion * (querynum + 1)):
            if i == len(params):
                break
            if not first:
                newquery += ', '
            else:
                first = False
            newquery += str(params[i])
        if newquery != querytext:
            queries.append(newquery)
    return queries


# =====================  Woring with SQL END=====================


# ====================== SQL write ======================


def get_original_name_from_request(request):
    result = request
    if result:
        position = result.find('?')
        if position:
            result = result[:position]
    return result


def get_metadata(session, base_url, request_timeout=60):
    metastructure = base_url + '$metadata'
    response = session.get(metastructure, timeout=request_timeout)
    if response.status_code == 200:
        metadata = dict()
        xml_text = response.text
        root = ET.fromstring(xml_text)
        standart_Odata = root[0][0]
        for element in standart_Odata:
            if not 'EntityType' in element.tag:
                continue
            meta = element.attrib.get('Name', None)
            if not meta:
                continue
            params = dict()
            for tag in element:
                metatype = tag.attrib.get('Type', None)
                if not metatype:
                    continue
                params[tag.attrib['Name']] = metatype
            metadata[meta] = params
        return metadata


def get_json(session, url, request_timeout=60):
    jsonquery_filter = '?$format=json;odata=nometadata&'
    if not jsonquery_filter in url:
        url = url.replace('?', jsonquery_filter)

    for _ in range(20):
        try:
            response = session.get(url, timeout=request_timeout)
            if response.status_code == 200:
                json_text = response.text
                root = json.loads(json_text)
                return root
        except Exception as E:
            logs(f'Connection error {E}- try {_}', 'info')
            sleep(60)
    logs(f'Connection error - cannot get info for url {url}', 'error')
    return None


def get_json_from_xml(session, url, request_timeout=60):
    res = list()

    for _ in range(20):
        try:
            response = session.get(url, timeout=request_timeout)
            if response.status_code == 200:
                xml_text = response.text
                root = ET.fromstring(xml_text)
                for element in root:
                    _rec = dict()
                    for sub in element:
                        if 'content' in sub.tag:
                            for field in sub[0]:
                                _tag = field.tag.replace('{http://schemas.microsoft.com/ado/2007/08/dataservices}',
                                                         '')

                                isNull = field.attrib.get(
                                    '{http://schemas.microsoft.com/ado/2007/08/dataservices/metadata}null', False)
                                if (len(field.attrib) != 0) and not isNull:
                                    _val = list()
                                    for _table in field:
                                        _el = dict()
                                        for sub_el in _table:
                                            _el_tag = sub_el.tag.replace(
                                                '{http://schemas.microsoft.com/ado/2007/08/dataservices}',
                                                '')
                                            _el_val = sub_el.text
                                            _el[_el_tag] = _el_val
                                        _val.append(_el)
                                else:
                                    _val = field.text
                                _rec[_tag] = _val
                            res.append(_rec)
                js = dict()
                js['value'] = res
                return js
        except Exception as E:
            logs(f'Connection error {E}- try {_}', 'info')
            sleep(60)
    logs(f'Connection error - cannot get info for url {url}', 'error')
    return None


def run(yaml_file):
    global verbose
    logs(f'Starting with {yaml_file}', 'info')
    with open(yaml_file, encoding='UTF-8') as file:
        settings = yaml.load(file, Loader=yaml.FullLoader)
    if not settings:
        logs(f'Error loading settings from {yaml_file}', 'error')
        return

    # read global settings for file
    global_config = settings['global_config']
    base_url = str(global_config['base_url']).strip()
    if base_url[:-1] != '/':
        base_url += '/'

    log_mode = str(global_config['log_mode']).strip().lower()

    json_allowed = bool(global_config.get('json_allowed', False))
    request_timeout = int(global_config.get('request_timeout', 60))

    if log_mode == "verbose":
        verbose = True

    # create session for HTTP-requests
    api_login = str(global_config['api_login']).strip()
    api_password = str(global_config['api_pwd']).strip()

    auth = HTTPBasicAuth(api_login, api_password)
    session = requests.Session()
    session.auth = auth

    # getting metadata for service
    metadata = get_metadata(session, base_url, request_timeout)
    tables = settings['tables']
    logs(f'found tables: {len(tables)}', 'info')

    # working with OData tables in yaml
    for table in tables:
        tabledict = tables[table]
        original_table = get_original_name_from_request(tabledict['data_request'])
        if not original_table:
            logs(f'No table for "{tabledict["data_request"]}"', 'info')
            continue
        logs(f'Working with {table}', 'info')

        # create new table or check if it exists
        query = get_create_table_query(table, original_table, metadata)
        execute_query(**global_config, query=query)

        # check fields in table equal to metadata
        # because 1c can be changed
        # if smth wrong - create table from scratch
        checked = checktable(table, original_table, metadata, **global_config)

        # full or period
        date_mode = tabledict['date_mode']

        if checked:
            # all fine - just read data
            json_url = base_url + tabledict["data_request"]
        else:
            # new table - use full data request
            j_request = tabledict.get('full_data_request', tabledict["data_request"])
            json_url = base_url + j_request
            # set mode to full
            date_mode = 'full'

        date_field = tabledict.get('date_field', '')
        requests_url = []
        if not date_field:
            # if we cannot use data field param - date mode is full
            # and we use only ode request
            date_mode = 'full'
            requests_url.append(json_url)
        else:
            startday_param = '#STARTDATE#'
            finishday_param = '#FINISHDATE#'
            date_inc = tabledict.get('date_inc', '1d')
            if date_mode == 'period':
                date_from = tabledict.get('date_from', str(date.today()))
                date_to = tabledict.get('date_to', str(date.today()))
            else:
                date_from = tabledict.get('date_from_full', str(date.today()))
                date_to = tabledict.get('date_to_full', str(date.today()))
            periods = generate_dates(date_from, date_to, date_inc)
            for period in periods:
                request_url = json_url.replace(startday_param, period[0])
                request_url = request_url.replace(finishday_param, period[1])
                requests_url.append(request_url)

        # clean table
        if date_mode == 'period':
            del_query = deleterows(table_name=table, date_field=date_field, date_from=str_to_date(date_from),
                                   date_to=str_to_date(date_to), all=False)
        else:
            # truncate all records
            del_query = deleterows(table_name=table, all=True)
        execute_query(**global_config, query=del_query)

        logs(f'   Requests to be sent: {len(requests_url)}', 'info')
        requests_count = 0
        # send request for each period
        for request_url in requests_url:
            requests_count += 1
            logs(f'   Sending {requests_count} of {len(requests_url)}', 'info')
            if json_allowed:
                # in json
                json_text = get_json(session, request_url, request_timeout)
            else:
                # in xml
                json_text = get_json_from_xml(session, request_url, request_timeout)
            if json_text:
                # if ok - write to sql by portions
                queries = get_insert_table_queries(table, json_text)
                logs(f'       Queries to be sent to SQL: {len(queries)}', 'info')
                queries_count = 0
                for each in queries:
                    queries_count += 1
                    logs(f'       Sending {queries_count} of {len(queries)}', 'info')
                    execute_query(**global_config, query=each)
    session.close()
    logs(f'Done: {yaml_file}', 'info')


verbose = False
today = str(date.today())
logging.basicConfig(filename=f'{today}.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
mask = '*.yaml'
for yaml_file in glob.glob(mask):
    try:
        run(yaml_file)
    except Exception as E:
        logs(f'{E} - cannot proceed {yaml_file}', 'error')
