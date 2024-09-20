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

load_dotenv('.env')

credentials = service_account.Credentials.from_service_account_file(os.environ['BQ_CRED'])
client = bigquery.Client(credentials = credentials)
rollbar_token = os.environ['ROLLBAR_TOKEN']
env_rollbar = 'production' if rollbar_token != '' else 'test'
rollbar.init(rollbar_token, env_rollbar)

def consult_order_mapping():
    mapp = pd.read_csv('old_consult_order_id.csv.gz')

    orders = pd.read_gbq("""
        select distinct o.id order_id, old_order_id from prod_l1_service_order.orders o
        where order_type = 'COMMERCE' and old_order_id is not null and order_status = 'COMPLETED'
    """, credentials = credentials)


    consult = pd.read_gbq("""
    select distinct o.id consultation_id, old_consult_id from prod_l1_service_medical.consultation o
    where old_consult_id is not null 
    """, credentials = credentials)


    orders['old_order_id'] = orders['old_order_id'].astype(float)
    mapp2 = mapp.merge(consult, how = 'left').merge(orders, how = 'left')
    mapp2 = mapp2[['consultation_id', 'old_consult_id', 'order_id', 'old_order_id'  ]].drop_duplicates()

    mapp2['old_order_id'] = mapp2['old_order_id'].astype('Int64')
    mapp2['old_consult_id'] = mapp2['old_consult_id'].astype('Int64')

    orders_consult = pd.read_gbq("""
    with cte as (
    select distinct o.id order_id,   cast(json_extract_scalar(metadata, '$.consult_prescription.consult_id') as integer) consultation_id,
    from prod_l1_service_order.orders o
    where ingest_date > '2024-02-01'  and order_status = 'COMPLETED' and old_order_id is null
                        )                

    select * from cte
    where consultation_id is not null
    """, credentials = credentials)

    all_consult_order_id = pd.concat([orders_consult, mapp2], ignore_index = 1)
    all_consult_order_id['old_consult_id'] = all_consult_order_id['old_consult_id'].astype('Int64')

    all_consult_order_id = all_consult_order_id.drop_duplicates(['order_id', 'consultation_id'])

    pandas_gbq.to_gbq(all_consult_order_id, 'ext_source.old_consult_order_mapping', project_id= 'good-doctor-titan-389604',credentials = credentials, chunksize=1000,
                    if_exists = 'replace')

gc = gspread.service_account(os.environ['CRED_GSHEET'])

def load_gsheet_req_data(gc):
  sht1 = gc.open_by_key('1HQZRLr-f5IcAH4H5hGs_kGeTwpSE7dvV6y2eojSkRkA')
  worksheet = sht1.worksheet("KAM Mapping")

  df_map = pd.DataFrame(worksheet.get_all_values(range_name = 'A2:F10000'))
  df_map.columns = ['payor_code', 'insurance_name', 'corporate_name',	'kam', 'month', 'aggregator']

  df_map_new = df_map[['payor_code', 'insurance_name']].drop_duplicates().dropna()

  pandas_gbq.to_gbq(df_map_new, 'ext_source.insurance_mapping_kam', credentials = credentials, 
                            if_exists = 'replace')


  sht2 = gc.open_by_key('1HQZRLr-f5IcAH4H5hGs_kGeTwpSE7dvV6y2eojSkRkA')
  worksheet = sht2.worksheet("old-insurance-az")

  df_map = pd.DataFrame(worksheet.get_all_values(range_name = 'A2:B10000'))
  df_map.columns = ['payor_name_old', 'insurance_name']

  df_map_new = df_map[['payor_name_old', 'insurance_name']].drop_duplicates().dropna()

  pandas_gbq.to_gbq(df_map_new, 'ext_source.insurance_mapping_old', credentials = credentials, 
                            if_exists = 'replace')



q_discharge_and_invoice = """
CREATE OR REPLACE table ext_source.discharge_and_invoice as
 with old_mapping as (
    select distinct id as consultation_id, old_consult_id  from `prod_l1_service_medical.consultation` c
    where old_consult_id  is not null and ingest_date < '2024-02-10'

    ), ppu as (

    select cast(coalesce(b.consultation_id, a.consultation_id) as integer) consultation_id ,
    rx_fee_ops_admin rx_total_ops, rx_fee_rx_excess rx_approved_price,
        consult_fee_ops consult_total_ops,consult_fee_consult_excess consult_approved_price,
        coshare, status_discharge, consultation_date, icdx, corporate_name, payor_name, card_number, claim_id  from `ext_source.all_b2b_ppu_trx` a
    left join old_mapping b on a.old_consultation_id = b.old_consult_id

    ), invoice as (
    select cast(coalesce(b.consultation_id, a.consult_id) as integer) consultation_id ,
    date as invoice_date, claim_id as invoice_claim_id, payor insurance_name, 
    corporate as invoice_corporate, order_id as invoice_order_id, order_id_2 as invoice_order_id2,
    approve_konsul as invoice_consult_fee, approve_obat invoice_rx_fee, invoice as invoice_id, final_status, icdx inv_icdx from `ext_source.final_invoice_ppu` a
    left join old_mapping b on a.old_consult_id = b.old_consult_id
    )


    select distinct coalesce(ppu.consultation_id, invoice.consultation_id) as consultation_id,
    cast(coalesce(invoice_date, consultation_date) as date) as invoice_date,
    ppu.* except(consultation_id, icdx, claim_id), invoice.* except(consultation_id, invoice_date, inv_icdx, insurance_name, invoice_corporate, invoice_claim_id),
    COALESCE(`inv_icdx`, `icdx`) icdx, COALESCE(`invoice_corporate`, `corporate_name`)  invoice_corporate,  insurance_name, 
        COALESCE(claim_id, invoice_claim_id) claim_id from ppu
    full join invoice on ppu.consultation_id = invoice.consultation_id;
"""

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



q_consult = """
  create or replace table ext_source.consult_and_order as
  with payor as (

      select distinct member_code, nahsehat_payor_code, p.name as payor_name, c.name as Corporate_name  from `prod_l1_service_payor.member` m
      left join  `prod_l1_service_payor.assigned_plan` a on a.id = m.assigned_plan_id 
      left join  `prod_l1_service_payor.payor` p on p.id = a.payor_id 
      left join `prod_l1_service_payor.corporate` c on c.id = a.corporate_id

      ),
      consult_raw as (

  SELECT a.id as consultation_id, a.created_at, a.updated_at, doctor_id, m.old_consult_id, m.order_id, 
        coalesce(cast(patient_profile_id as string), cast(pr.id as string)) profile_id, patient_user_id user_id,
       json_extract_scalar(insurance_data, '$.total') as total,   
        coalesce(pr.name, json_extract_scalar(patient_profile_data,'$.name')) as patient_name
        ,d.name as doctor_dpt_name, consultation_status, od.order_status,'CONSULTATION' order_type, json_extract_scalar(insurance_data, '$.member_code') member_code, 
          case when d.id in (1,28) then 'GP' else 'SP' end as consultation_type,
        nahsehat_payor_code, payor_name, Corporate_name,
        row_number() over(partition by a.id order by a.created_at desc) as rn
      FROM prod_l1_service_medical.consultation a
      -- left join (select distinct consultation_id, 
      --   status_discharge from ext_source.discharge_and_invoice) di on a.id = di.consultation_id
      left join ext_source.old_consult_order_mapping m on a.id = m.consultation_id
      left join prod_l1_service_order.orders od on m.order_id = od.id
      left join payor p on p.member_code = json_extract_scalar(insurance_data, '$.member_code')
      left join prod_l1_service_medical.doctor b on a.doctor_id = b.id
      left join prod_l1_service_medical.department d on a.department_id = d.id
      left join prod_l1_service_users.profiles pr on pr.code = json_extract_scalar(a.patient_profile_data, '$.pagd_user_id')
      ), consult as (
        select * except(rn) from consult_raw
        where rn = 1),
      first_trx as (
          with cte as (
        select consultation_id, created_at as first_trx_date, profile_id,
            row_number() over( partition by profile_id, consultation_status order by created_at) as rn from
            consult
            where
              order_type = 'CONSULTATION'
              and consultation_status = 'COMPLETED'
        ), cte2 as (

        select profile_id, max(rn) max_trx from cte
        group by 1

      )

      select distinct cte.profile_id, first_trx_date, max_trx, 
        case when max_trx = 1 then '1 Consult'
          when max_trx <= 3 then '2-3 Consults'
          when max_trx > 3 then 'More than 3 Consults' end as consult_per_user
                from cte
      left join cte2 on cte.profile_id =cte2.profile_id
      where rn = 1


      )
      , funnel as (

        select c.consultation_id, max(case when p.id is not null or o.order_id is not null then 1 else 0 end) rx_flag, 
                max(case when o.order_id is not null then 1 else 0 end) as order_flag, 
                  max(case when lower(status_discharge) like '%success%' then 1 else 0 end) success_flag  from consult c
        left join `ext_source.old_consult_order_mapping` o on c.consultation_id = o.consultation_id
        left join `good-doctor-titan-389604.prod_l1_service_order.orders` od on od.id = o.order_id
        left join `good-doctor-titan-389604.prod_l1_service_medical.prescription`  p on p.consult_id = c.consultation_id
        left join (select distinct consultation_id, 
          status_discharge from ext_source.discharge_and_invoice) di on c.consultation_id = di.consultation_id  
          group by 1
      ), additional_data as (
        with consultation as (
          with main_data as (
            select id as consultation_id, json_extract_scalar(patient_profile_data, '$.birth_date') dob, 
                json_extract_scalar(patient_profile_data, '$.pagd_user_id') pagd_user_id,
              coalesce(cast(json_extract_scalar(payment, '$.sub_total') as numeric), 0) as consult_fee, 
                    row_number() over(partition by id order by updated_at desc) rn from prod_l1_service_medical.consultation
            -- where ingest_date > '2022-02-01'
            )
            select * except(rn) from main_data
            where rn = 1 
        ), ord as (
        with main_data as (
          select id order_id,updated_at, cast(json_extract_scalar(metadata, '$.consult_prescription.consult_id') as integer) as consult_id,
            o.rebooking_total, o.rebooking_benefit_coverage_amount, o.total, o.benefit_coverage_amount, 
            coalesce(o.voucher_coverage_amount, 0) voucher_coverage_amount,
            row_number() over(partition by id order by updated_at desc) rn from prod_l1_service_order.orders o
          where -- ingest_date > '2022-02-01' and 
            order_status = 'COMPLETED'
          )
          select * except(rn) from main_data
          where rn = 1 
        ), coms_ord as (
        with main_data as (
          select order_id, co.rebooking_shipping_fee, co.shipping_fee,  co.shipping_fee_markup ,
          row_number() over(partition by order_id order by order_id desc) rn from `prod_l1_service_order.commerce_orders` co
          -- where ingest_date > '2022-02-01' 
          )
          select * except(rn) from main_data
          where rn = 1 
        ), order_all as (
        select o.consult_id,  o.order_id, updated_at, case 
            when o.rebooking_total is not null 
              then o.rebooking_total+o.rebooking_benefit_coverage_amount - co.rebooking_shipping_fee - co.shipping_fee_markup
            else o.total + o.benefit_coverage_amount- co.shipping_fee- co.shipping_fee_markup+coalesce(o.voucher_coverage_amount, 0)
          end as prescription_fee 
        from ord o 
        left join coms_ord co on o.order_id = co.order_id

        ), order_summ as(
          with clean_ord as (
            select *, row_number() over(partition by order_id order by updated_at desc) rn from order_all
          )
          select consult_id, sum(prescription_fee) prescription_fee from clean_ord
          where rn = 1
          group by consult_id
        ), dob_old as (
          select code, birth_date  from `prod_l1_service_users.profiles`
          where code is not null

        ), cd as (
          select
                  distinct cd.consultation_id
                    , i.code
                from 
                    prod_l1_service_medical.consult_diagnosis as cd 
                    left join prod_l1_service_medical.icd as i on i.id = cd.icd_id
        ), cdx as (
          select consultation_id, string_agg(code, ',') icdx
                from cd
                group by 1
        ), final as (
          select  c.* except(pagd_user_id, dob),
            os.* except(consult_id), safe_cast(coalesce(dob, cast(birth_date as string)) as date) dob,
            cdx.* except(consultation_id), row_number() over(partition by c.consultation_id order by c.consultation_id) rn 
            from consultation c
          left join order_summ os on c.consultation_id = os.consult_id
          left join dob_old on dob_old.code = c.pagd_user_id
          left join cdx on cdx.consultation_id = c.consultation_id 
          )

        select * except(rn) from final
        where rn = 1

      )

select a.*,  c.* except(profile_id)
, f.* except(consultation_id), ad.* except(consultation_id)
from 
  consult a
left join first_trx c on a.profile_id = c.profile_id
left join funnel f on a.consultation_id = f.consultation_id
left join additional_data ad on a.consultation_id = ad.consultation_id;


"""


q_client_portal = """
create or replace table ext_source.client_portal as
    with consult as (
      select * except(payor_name, Corporate_name), payor_name payor_name_titan, Corporate_name corporate_name_titan, 
        row_number() over(partition by consultation_id order by created_at desc) rn from ext_source.consult_and_order

    ), disc as (
        select *,
        row_number() over(partition by consultation_id order by invoice_date desc) rn from ext_source.discharge_and_invoice

    ), consult_clean as (
      select * except(rn) from consult 
      where rn = 1
    ), disc_clean as (
      select * except(rn) from disc 
      where rn = 1
    ),
    icdx_clean as (
      select consultation_id, STRING_AGG(icdx, ',') icdx  from ext_source.icdx 
      group by 1 
    )
    , mrg_clean as (

      select a.* except(nahsehat_payor_code, member_code),
         b.* except(consultation_id, insurance_name, invoice_date, card_number), 
        coalesce(a.member_code, b.card_number) member_code,
        lower(nahsehat_payor_code) nahsehat_payor_code, 
          coalesce(lower(trim(insurance_name)), lower(trim(payor_name))) insurance_name,
            date_add(created_at, interval 7 hour) created_at_jkt,
            date_add(updated_at, interval 7 hour) updated_at_jkt,
            case when coalesce(a.member_code, b.card_number) is not null or status_discharge is not null then 1 else 0 end is_b2b_trx_new,
            COALESCE(COALESCE(`invoice_rx_fee`,`rx_approved_price`), 0) + 
              COALESCE(COALESCE(`invoice_consult_fee`,`consult_approved_price`), 0) claim_amount_new,
              case when COALESCE(COALESCE(`invoice_date`, cast(`consultation_date` as date)), 
                                    cast(date_add(created_at, interval 7 hour) as date)) < '2022-01-01'
                then cast(date_add(created_at, interval 7 hour) as date)
                else COALESCE(COALESCE(`invoice_date`, cast(`consultation_date` as date)), 
                                    cast(date_add(created_at, interval 7 hour) as date)) end as  invoice_date from consult_clean a
      left join disc_clean b on a.consultation_id = b.consultation_id

    ), map as (
      select distinct lower(trim(payor_code)) payor_code, insurance_name insurance_name_clean from `ext_source.insurance_mapping_kam`

    ), map1 as (
      select distinct lower(trim(insurance_name)) insurance_missing_1 from `ext_source.insurance_mapping_kam`
    ), map2 as (
        select distinct lower(trim(payor_name_old)) payor_name_old, insurance_name insurance_missing_2 from `ext_source.insurance_mapping_old`

    ), main as (
      select a.* except(icdx) ,  i.icdx,
      -- case when lower(a.status_discharge) like '%success%' then 
         UPPER(trim(coalesce(coalesce(coalesce(coalesce(coalesce(b.insurance_name_clean, c.insurance_name_clean), 
             d.insurance_missing_1), e.insurance_missing_2), payor_name), payor_name_titan))) 
             -- else NULL end
              as `Insurance Name`,
             row_number() over(partition by a.consultation_id order by created_at) rn from mrg_clean a
      left join map b on a.insurance_name = b.payor_code
      left join map c on a.nahsehat_payor_code = c.payor_code
      left join map1 d on a.insurance_name = d.insurance_missing_1
      left join map2 e on a.insurance_name = e.payor_name_old
      left join icdx_clean i on i.consultation_id = a.consultation_id

    ), main_ins as (

    select * except(`Insurance Name`, rn), case when `Insurance Name` = UPPER('PT Teknologi Pamadya Analitika (MEDITAP)') then UPPER(TRIM(`payor_name`))
           when `Insurance Name` = 'AAA' then UPPER(TRIM(`payor_name`))
           when `Insurance Name` = 'MEDITAP' then UPPER(TRIM(`payor_name`))  else 
`Insurance Name` end as `Insurance Name` from main
where rn = 1
)

select * except(is_b2b_trx_new), case when is_b2b_trx_new = 1 or  `Insurance Name` is not null then 1 else 0 end is_b2b_trx_new from main_ins


"""


q_fraud_flagging = """

create or replace table ext_source.fraud_flagging as 
    with fraud_icdx as(
    with ctex as(

      select consultation_id, date(invoice_date) dt, profile_id, icdx, member_code as card_number, rx_approved_price, consult_approved_price from ext_source.client_portal
      where (icdx is not null or icdx <> '' or icdx <> 'nan') and lower(status_discharge) like '%success%'

    ), ctex2 as (
      select *, row_number() over(partition by consultation_id order by dt desc) rn from ctex
      

    ), cte as (

      select ctex2.* except(rn, icdx), icdx from ctex2, UNNEST(SPLIT(icdx, ',')) icdx   
      where rn = 1

    )
    , cte2 as (
    select *, lag(dt) over(partition by profile_id, icdx order by dt) dt_lag1 from cte

    ), cte3 as (
    select *, lag(dt_lag1) over(partition by profile_id, icdx order by dt_lag1) dt_lag2 from cte2

    )
    ,  cte4 as (

    select *, date_diff(dt, dt_lag1, day) diff_lag1, date_diff(dt, dt_lag2, day) diff_lag2 from cte3
    where dt_lag2 is not null

    ), cte5 as (
    select profile_id, dt, dt_lag1, diff_lag2 from cte4
    where cte4.diff_lag1 <= 7 and cte4.diff_lag2 <=7

    ), cte6 as (

    SELECT distinct profile_id,
      SAFE_CAST(value AS Date) dt
    FROM (
      SELECT profile_id,
        REGEXP_REPLACE(SPLIT(pair, ':')[OFFSET(1)], r'^"|"$', '') value 
      FROM cte5 t, 
      UNNEST(SPLIT(REGEXP_REPLACE(to_json_string(t), r'{|}', ''))) pair
    )
    where SAFE_CAST(value AS Date) is not null
    ), cte7 as (


    select a.*, row_number() over(partition by a.profile_id, a.icdx order by a.dt) rn from cte3 a
    inner join cte6 b on a.profile_id = b.profile_id
      and a.dt = b.dt 
      

      ), cte8 as (

      select * from cte7
      where rn = 3
        ), cte9 as (

      select  *, row_number()over(partition by consultation_id order by dt asc) rn
      from(
      select a.* from cte3 a
      inner join cte8 b on a.profile_id = b.profile_id 
      and a.icdx = b.icdx and a.dt = b.dt
      union distinct
      select a.* from cte3 a
      inner join cte8 b on a.profile_id = b.profile_id 
      and a.icdx = b.icdx and a.dt = b.dt_lag1
      union distinct
      select a.* from cte3 a
      inner join cte8 b on a.profile_id = b.profile_id 
      and a.icdx = b.icdx and a.dt = b.dt_lag2
      ) z
      order by profile_id , dt, icdx

        )

    select * except(rn) from cte9
    where rn =1

    ), fraud_pres as (
    with ctex as(
      select consultation_id, date(invoice_date) dt, profile_id, icdx, member_code card_number, rx_approved_price, consult_approved_price,
         case when rx_approved_price > 0 then 1 else 0 end prescription_flag from ext_source.client_portal
      where lower(status_discharge) like '%success%'
    ), ctex2 as (
      select *, row_number() over(partition by consultation_id order by dt desc) rn from ctex
      

    ), cte as (

      select * except(rn) from ctex2
      where rn = 1

    )
    , cte2 as (
    select *, lag(dt) over(partition by profile_id, prescription_flag order by dt) dt_lag1 from cte

    ), cte3 as (
    select *, lag(dt_lag1) over(partition by profile_id, prescription_flag order by dt_lag1) dt_lag2 from cte2

    ),  cte4 as (

    select *, date_diff(dt, dt_lag1, day) diff_lag1, date_diff(dt, dt_lag2, day) diff_lag2 from cte3
    where dt_lag1 is not null

    ), cte5 as (
    select profile_id, dt, dt_lag1, dt_lag1 from cte4
    where cte4.diff_lag1 <= 7 and cte4.diff_lag2 <= 7

    ), cte6 as (

    SELECT distinct profile_id,
      SAFE_CAST(value AS Date) dt
    FROM (
      SELECT profile_id,
        REGEXP_REPLACE(SPLIT(pair, ':')[OFFSET(1)], r'^"|"$', '') value 
      FROM cte5 t, 
      UNNEST(SPLIT(REGEXP_REPLACE(to_json_string(t), r'{|}', ''))) pair
    )
    where SAFE_CAST(value AS Date) is not null
    ), cte7 as (


    select a.*, row_number() over(partition by a.profile_id, a.prescription_flag order by a.dt) rn from cte3 a
    inner join cte6 b on a.profile_id = b.profile_id
      and a.dt = b.dt 
      

      ), cte8 as (

      select * from cte7
      where rn = 3
        ), cte9 as (

      select  *, row_number()over(partition by consultation_id order by dt asc) rn
      from(
      select a.* from cte3 a
      inner join cte8 b on a.profile_id = b.profile_id 
      and a.prescription_flag = b.prescription_flag and a.dt = b.dt
      union distinct
      select a.* from cte3 a
      inner join cte8 b on a.profile_id = b.profile_id 
      and a.prescription_flag = b.prescription_flag and a.dt = b.dt_lag1
        union distinct
      select a.* from cte3 a
      inner join cte8 b on a.profile_id = b.profile_id 
      and a.prescription_flag = b.prescription_flag and a.dt = b.dt_lag2
      ) z
      order by profile_id , dt, prescription_flag

        )

    select * except(rn) from cte9
    where rn =1

    ), fraud_consult as (
    with cte as (
    select consultation_id, date(invoice_date) dt, profile_id, icdx, member_code as card_number, rx_approved_price, rx_approved_price from ext_source.client_portal
    where lower(status_discharge) like '%success%'
    ), cte2 as (
      select profile_id, dt, count(consultation_id) cnt from cte
      group by 1, 2
      having count(consultation_id) > 2
    )


    select a.*, b.cnt consult_in_a_day from cte a
    inner join cte2 b on a.profile_id = b.profile_id and  a.dt = b.dt
    where cnt is not null
    ), main as (
    select distinct coalesce(coalesce(f1.consultation_id, f2.consultation_id), f3.consultation_id) as consultation_id,
    coalesce(coalesce(f1.dt, f2.dt), f3.dt) as date,
    coalesce(coalesce(f1.profile_id, f2.profile_id), f3.profile_id) as profile_id,
    case when consult_in_a_day > 2 then 1 end as is_consult_in_a_day, case when prescription_flag is not null then 1 end as multiple_prescription_order, 
      case when f1.icdx is not null then 1 end as multiple_icdx,
    from fraud_icdx f1
    full join fraud_pres f2 on f1.consultation_id = f2.consultation_id
    full join fraud_consult f3 on f1.consultation_id = f3.consultation_id
    order by profile_id, date

    )

select * from main a
where profile_id is not null and profile_id <> ''

"""


q_master_data = """
create or replace table ext_source.domo_trx_master_data as
    with cte as (
    select consultation_id, max(is_consult_in_a_day) is_consult_in_a_day, 
        max(multiple_prescription_order) multiple_prescription_order,
        max(multiple_icdx) multiple_icdx  from ext_source.fraud_flagging
        group by 1
    ), old_mapping as (

    select a.consultation_id from ext_source.old_consult_order_mapping a
    inner join ext_source.old_b2c_trx b on a.old_consult_id = cast(b.old_consult_id as integer)

  )


    select a.* except(consultation_status, order_status), consultation_status as order_status, b.* except(consultation_id) from `ext_source.client_portal` a
    left join cte b on a.consultation_id = b.consultation_id
    left join old_mapping c on a.consultation_id = c.consultation_id
    where is_b2b_trx_new = 1 or c.consultation_id is not null or a.created_at >= '2024-02-07'

"""





try :
    load_gsheet_req_data(gc)  # API request

    consult_order_mapping()
except:
    rollbar.report_exc_info()

res = client.query(q_discharge_and_invoice)  # API request
if res.errors:
    raise Exception('Table ext_source.discharge_and_invoice not successfullly refreshed in bigQuery')
    rollbar.report_exc_info()

time.sleep(30)
try :
  icdx_cleaning(credentials)  # API request
except:
    rollbar.report_exc_info()


res = client.query(q_consult)  # API request
if res.errors:
    raise Exception('Table ext_source.consult_and_order not successfullly refreshed in bigQuery')
    rollbar.report_exc_info()

time.sleep(60)

res = client.query(q_client_portal)  # API request
if res.errors:
    raise Exception('Table ext_source.client_portal not successfullly refreshed in bigQuery')
    rollbar.report_exc_info()

time.sleep(60)

res = client.query(q_fraud_flagging)  # API request
if res.errors:
    raise Exception('Table ext_source.fraud_flagging not successfullly refreshed in bigQuery')
    rollbar.report_exc_info()

time.sleep(30)

res = client.query(q_master_data)  # API request
if res.errors:
    raise Exception('Table ext_source.domo_trx_master_data not successfullly refreshed in bigQuery')
    rollbar.report_exc_info()

