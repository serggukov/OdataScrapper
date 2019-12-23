# OdataScrapper
Scrapping from 1C Odata and BPM Odata and transfer it to MS SQL

Description for config files

All congiguration files must be *.yaml with next structure
All string values must be enclosed in double quotes

====== FOR 1C ======
global_config:
    ms_sql_db_host: MS SQL host name
    ms_sql_db: database name in MS SQL
    ms_sql_db_user: user login in MS SQL
    ms_sql_db_pass: user password in MS SQL
    log_mode: "verbose" or "" - to verbose or silent mode
    base_url: OData service home URL. Should end by "/". If not - it will added automatically
    api_login: OData login
    api_pwd: OData password
    json_allowed: 1 if OData can provide data in JSON (1C 8.3.5+) or 0 in xml only. 
    request_timeout: timeout delay (for long requests. 60 by default)

tables:
    table1: - table name in SQL
        data_request: OData-request. Will be added to base url. If there is period field in table - higthy recommend to use it - for example  "$filter=Created ge datetime'#STARTDATE#' and Created le datetime'#FINISHDATE#'"
        full_data_request: OData-request without any filters (except period). If empty  - will use prev.parameter
	      date_mode: "full" or "period". If "full" - table in MS SQL will be truncated, else only deleted by period.
        date_field: "" - field name for period field. 
        date_from: "2016-01-01" - will scrap from date
        date_to: "2019-10-31" -  will scrap till date
        date_from_full: "2016-01-01" - will scrap from date in full mode
        date_to_full: "2019-10-31" - will scrap till date in full mode
        date_inc: "1w" - count and type of periods. y – 365 days, m – 30 days, w – 7 days, d – 1 day

 

====== FOR BPM ======

global_config:
    ms_sql_db_host: MS SQL host name
    ms_sql_db: database name in MS SQL
    ms_sql_db_user: user login in MS SQL
    ms_sql_db_pass: user password in MS SQL
    log_mode: "verbose" or "" - to verbose or silent mode
    base_url: OData service home URL. Should end by "/". If not - it will added automatically
    api_login: OData login
    api_pwd: OData password
    request_timeout: timeout delay (for long requests. 60 by default)

tables:
    table1: - table name in SQL
        data_request: OData-request. Will be added to base url. 
