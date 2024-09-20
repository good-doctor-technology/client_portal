import pandas_gbq
import pandas as pd
import numpy as np
from google.oauth2 import service_account
import gspread 
from dotenv import load_dotenv
import os 
from google.cloud import bigquery
import rollbar
import time

load_dotenv('/root/data-team-adhoc-script/.env')

con_bh = os.environ['bh_con']


q = """
    select consult_id old_consult_id, consult_requested_dt  from rpt_gdti_consult_internal_details
    where b2b_nonlinked is null and b2b_linked is null and b2b_trx is null
    and consult_requested_dt >= '2022-01-01'
    union
    select consult_id old_consult_id, consult_requested_dt from rpt_gdti_consult_external_details rgced
    where b2b_nonlinked is null and b2b_linked is null and b2b_trx is null
    and consult_requested_dt >= '2022-01-01'
"""

df = pd.read_sql(q, con_bh)

credentials = service_account.Credentials.from_service_account_file(os.environ['BQ_CRED'])

pandas_gbq.to_gbq(df, 'ext_source.old_b2c_trx', credentials = credentials, if_exists = 'replace')