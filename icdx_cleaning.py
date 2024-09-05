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

def icdx_cleaning(credentials):
    icdx_query = """
    with a as (

    select consultation_id, upper(regexp_replace(trim(replace(replace(replace(c, '"', ''), '\\'', ''), 'nan', '')), '\\\.$', '')) as icdx from `ext_source.discharge_and_invoice`,
    UNNEST(SPLIT(icdx, ',')) c
    ), b as (
    select consultation_id, case when regexp_contains(icdx, '[a-zA-Z]') and regexp_contains(icdx, '[0-9]')  then c else ''  end as icdx from a,
    UNNEST(SPLIT(icdx, '&')) c
    ), c as (
        select consultation_id, trim(case when length(c) > 10 then '' else c end) as icdx from b,
        UNNEST(SPLIT(icdx, ' ')) c
    ), d as (

            select consultation_id, case when icdx = 'J00K29.7' then 'J00,K29.7'
            when icdx = 'J00A69.0' then 'J00,A69.0'
            when icdx = 'A09Z71.0' then 'A09,Z71.0'
            when icdx = 'K30.A01' then 'K30,A01'
            when icdx = 'H10.L66.4' then 'H10,L66.4'
            when icdx = 'L50.K30' then 'L50,K30'
            when icdx = 'K30.J00' then 'K30,J00'
            when icdx = 'R50.9R51' then 'R50.9,R51'
            when icdx = 'J00R05K30' then 'J00,R05,K30'
            when icdx = 'H61.2H60.8' then 'H61.2,H60.8'
            when icdx = 'E78.0E79.0' then 'E78.0,E79.0'
            when icdx = 'J00.J02.9' then 'J00,J02.9'
            when icdx = 'M54.5Z09.8' then 'M54.5,Z09.8'
            when icdx = 'K05.6Z09.8' then 'K05.6Z09.8'
            when icdx = 'J06.9J02.9' then 'J06.9,J02.9'
            when icdx = 'L23.9Z09.8' then 'L23.9,Z09.8'
            when icdx = 'B35.1B35.4' then 'B35.1,B35.4'
            when icdx = 'K30R50.9' then 'K30,R50.9'
            when icdx = 'J00R50.9' then 'J00,R50.9'
            when icdx = 'N39.0Z09.7' then 'N39.0,Z09.7'
            when icdx = 'J00K52.9' then 'J00,K52.9'
            when icdx = 'J45.9Z09.8' then 'J45.9,Z09.8'
            when icdx = 'R50.9J00' then 'R50.9,J00'
            when icdx = 'A15.0Z09.8' then 'A15.0,Z09.8'
            when icdx = 'M72.2Z09.8' then 'M72.2,Z09.8'
            when icdx = 'J00G43.0' then 'J00,G43.0'
            when icdx = 'J06.9K30' then 'J06.9,K30'
            when icdx = 'M72.202' then 'M72.2'
            when icdx = 'R42.R51' then 'R42,R51'
            when icdx = 'K30.R11' then 'K30,R11'
            when icdx = 'L50.0Z09.8' then 'L50.0,Z09.8'
            when icdx = 'N75.1N75.0' then 'N75.1,N75.0'
            when icdx = 'Z71.0H10.3' then 'Z71.0,H10.3'
            when icdx = 'J00Z71.0' then 'J00,Z71.0'
            when icdx = 'Z71.0J30.0' then 'Z71.0,J30.0'
            when icdx = 'M10.9M79.1' then 'M10.9,M79.1'
            when icdx = 'I15Z09.8' then 'I15,Z09.8'
            when icdx = 'N61.X04' then 'N61,X04'
            when icdx = 'J11.R50.9' then 'J11,R50.9'
            when icdx = 'N76.0B35.6' then 'N76.0,B35.6'
            when icdx = 'J02.8J00' then 'J02.8,J00'
            when icdx = 'J00H10.3' then 'J00,H10.3'
            when icdx = 'K12.0A69.1' then 'K12.0,A69.1'
            when icdx = 'K52.9.9' then 'K52.9.9'
            when icdx = 'R10.0N94.6' then 'R10.0,N94.6'
            when icdx = 'K52.8J00' then 'K52.8,J00'
            when icdx = 'N39.0Z09.8' then 'N39.0,Z09.8'
            when icdx = 'G44.2Z09.8' then 'G44.2,Z09.8'
            when icdx = 'B35.6Z09.8' then 'B35.6,Z09.8'
            when icdx = 'K59.1A09' then 'K59.1,A09'
            when icdx = 'M79.1M54.5' then 'M79.1,M54.5'
            when icdx = 'R50.9R10.4' then 'R50.9,R10.4'
            when icdx = 'G43.9Z09.8' then 'G43.9,Z09.8'
            when icdx = 'K59.0K30' then 'K59.0,K30'
            when icdx = 'B35.6B35.3' then 'B35.6,B35.3'
            when icdx = 'N39.9Z09.8' then 'N39.9,Z09.8'
            when icdx = 'S09.9Z09.8' then 'S09.9,Z09.8'
            when icdx = 'Z71.0Z00.2' then 'Z71.0,Z00.2'
            when icdx = 'K62.5Z09.8' then 'K62.5,Z09.8'
            when icdx = 'E78.0Z09.8' then 'E78.0,Z09.8'
            when icdx = 'K30.R50.9' then 'K30,R50.9'
            when icdx = 'F51.F32.9' then 'F51,F32.9'
            when icdx = 'K02.9Z71.3' then 'K02.9,Z71.3'
            when icdx = 'A01.4A09' then 'A01.4,A09'
            when icdx = 'E78.0Z71.9' then 'E78.0,Z71.9'
            when icdx = 'R52K04.0' then 'R52,K04.0'
            when icdx = 'Z71.0A09' then 'Z71.0,A09'
            when icdx = 'G43Z09.8' then 'G43,Z09.8'
            when icdx = 'K12.0Z09.8' then 'K12.0,Z09.8'
            when icdx = 'N39.0N21.9' then 'N39.0,N21.9'
            when icdx = 'T14.9Z09.8' then 'T14.9,Z09.8'
            when icdx = 'T01.9M79.1' then 'T01.9,M79.1'
            when icdx = 'R50.9L98.9' then 'R50.9,L98.9'
            when icdx = 'L30.9G37.2' then 'L30.9,G37.2'
            when icdx = 'R10.4N94.6' then 'R10.4,N94.6'
            when icdx = 'R50.9L30.9' then 'R50.9,L30.9'
            when icdx = 'K30G44.2' then 'K30,G44.2'
            when icdx = 'R52.9Z09.8' then 'R52.9,Z09.8'
            when icdx = 'J01.2Z09.8' then 'J01.2,Z09.8'
            when icdx = 'L08.9Z09.8' then 'L08.9,Z09.8'
            when icdx = 'D10-D36' then 'D10,D36'
            when icdx = 'R50K59.1' then 'R50,K59.1'
            when icdx = 'H10.K59.0' then 'H10,K59.0'
            when icdx = 'H60Z09.8' then 'H60,Z09.8'
            when icdx = 'Z71.0J00' then 'Z71.0,J00'
            when icdx = 'K59.1K30' then 'K59.1,K30'
            when icdx = 'J00.K30' then 'J00,K30'
            when icdx = 'B34.2Z09.8' then 'B34.2,Z09.8'
            when icdx = 'A09.0Z09.8' then 'A09.0,Z09.8'
            when icdx = 'K59.0Z09.8' then 'K59.0,Z09.8'
            when icdx = 'J00K12.0' then 'J00,K12.0'
            when icdx = 'R05.R50.9' then 'R05,R50.9'
            when icdx = 'J00J02.9' then 'J00,J02.9'
            when icdx = 'B34.9Z09.8' then 'B34.9,Z09.8'
            when icdx = 'A01.2J06.9' then 'A01.2,J06.9'
            when icdx = 'K59.1Z09.8' then 'K59.1,Z09.8'
            when icdx = 'A09Z09.8' then 'A09,Z09.8'
            when icdx = 'R11.K30' then 'R11,K30'
            when icdx = 'M13.9Z09.8' then 'M13.9,Z09.8'
            when icdx = 'E14.900' then 'E14. 9'
            when icdx = 'R50.9Z09.8' then 'R50.9,Z09.8'
            when icdx = 'J02.9Z09.8' then 'J02.9,Z09.8'
            when icdx = 'M79.1Z09.8' then 'M79.1,Z09.8'
            when icdx = 'J00J02.0' then 'J00,J02.0'
            when icdx = 'J00.R50.9' then 'J00,R50.9'
            when icdx = 'K30Z09.8' then 'K30,Z09.8'
            when icdx = 'J06.9Z09.8' then 'J06.9,Z09.8'
            when icdx = 'J00Z09.8' then 'J00,Z09.8'
            when icdx = 'J00K30' then 'J00,K30'
            when icdx = 'R51J00' then 'R51,J00'
            when icdx = 'K30R42' then 'K30,R42'
            when icdx = 'R42R11' then 'R42,R11'
            when icdx = 'R05R11' then 'R05,R11'
            when icdx = 'K30J00' then 'K30,J00'
            when icdx = 'J00I10' then 'J00,I10'
            when icdx = 'K30R11' then 'K30,R11'
            when icdx = 'R51K30' then 'R51,K30' else icdx end as icdx
            from c
    ), e as (

    select consultation_id, case when regexp_contains(icdx, '[a-zA-Z]') and regexp_contains(icdx, '[0-9]')  then c 
                                when length(c)  < 3 then '' else ''  end as icdx from d,
    UNNEST(SPLIT(icdx, ',')) c

    )

    select consultation_id,icdx from e



    """
    df = pd.read_gbq(icdx_query, credentials = credentials)
    df['icdx'] = df['icdx'].str.replace("\\.$", '', regex = True).str.replace("^\\.", "", regex = True)
    df['icdx'] = df['icdx'].str.replace('[', '').str.replace(']', '').str.replace('\\', '').str.replace('/', '').str.replace('', '')
    df['icdx'] = df['icdx'].str.replace('..', '').str.replace('.', '')
    df['icdx'] = np.where((df['icdx'].str.len() <= 6) & (df['icdx'].str.len() >= 3), 
                            df['icdx'], None)
    pandas_gbq.to_gbq(df, 'ext_source.icdx', credentials = credentials, if_exists = 'replace')
    return df

if __name__ == "__main__":
    credentials = service_account.Credentials.from_service_account_file(os.environ['BQ_CRED'])
    icdx_cleaning(credentials)
