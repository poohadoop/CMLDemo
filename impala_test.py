#!pip install thrift==0.9.3
#!pip install impyla==0.16.2
#!pip install thrift_sasl

import pandas
import os

from impala.dbapi import connect
from impala.util import as_pandas

host = 'coordinator-lgu-impala-medium.env-67np6k.dwx.cloudera.site'
conn = connect(host, 443, use_ssl=True, use_http_transport=True, http_path='cliservice',
                      database='tpcds_parquet_1000', user='trial2101', password = 'Cloudera@01', auth_mechanism="LDAP")

#print conn
# Execute using SQL
cursor = conn.cursor()
#print cursor
cursor.execute('select * from default.creditcard limit 100')

# Pretty output using Pandas
tables = as_pandas(cursor)
tables

#result=cursor.fetchall()
#for row in results:
#  print row