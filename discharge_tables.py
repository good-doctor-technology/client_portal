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
import gc

load_dotenv('/root/data-team-adhoc-script/.env')

credentials = service_account.Credentials.from_service_account_file(os.environ['BQ_CRED'])
client = bigquery.Client(credentials = credentials)
rollbar_token = os.environ['ROLLBAR_TOKEN']
env_rollbar = 'production' if rollbar_token != '' else 'test'
rollbar.init(rollbar_token, env_rollbar)


con_ord = os.environ['con_ord']
con_pay = os.environ['con_pay']
con_med = os.environ['con_med']

q_consult_new = """
    with 
        cd as 
            (
            select
                cd.consultation_id
                , i.code as icd 
                , i.name_id as diagnosis
                , row_number() over(partition by cd.consultation_id order by cd.icd_id) as rn
            from 
                consult_diagnosis as cd 
                left join icd as i on i.id = cd.icd_id
            where 
                cd.created_at at time zone 'Asia/Jakarta' >= date(current_timestamp at time zone 'Asia/Jakarta' - interval '7' day)
            )

    select 
        date(c.created_at at time zone 'Asia/Jakarta') as consultation_date
        , c.id
        , date(c.end_time at time zone 'Asia/Jakarta') as end_date
        , c.est_end_time at time zone 'Asia/Jakarta' as est_end_time 
        , to_char(c.start_time at time zone 'Asia/Jakarta', 'hh24:MI:ss') as start_time
        , to_char(c.end_time at time zone 'Asia/Jakarta', 'hh24:MI:ss') as end_time
        , case when dp.id in (1,28) then 'GP' else 'SP' end as gp_or_sp
        , case when c.consultation_status = 'ONGOING' then 'COMPLETED' else cast(c.consultation_status as varchar) end as "consultation_status"
        , c.reference_number
        , case when p.id is null then false else true end as prescription
        , jsonb_extract_path_text(c.payment, 'method') as payment_method
        , coalesce(cast(jsonb_extract_path_text(c.payment, 'sub_total') as numeric), 0) as consult_fee
        , coalesce(cast(jsonb_extract_path_text(c.payment, 'admin_fee') as numeric), 0) as admin_fee
        , coalesce(cast(jsonb_extract_path_text(c.payment, 'discount_amount') as numeric), 0) as discount
        , coalesce(cast(jsonb_extract_path_text(c.payment, 'benefit_coverage_amount') as numeric), 0) as benefit_amount
        , coalesce(cast(jsonb_extract_path_text(c.payment, 'voucher_coverage_amount') as numeric), 0) as voucher_amount
        , coalesce(cast(jsonb_extract_path_text(c.payment, 'total') as numeric), 0) as user_paid_amount
        , null as consult_fee_co_share
        , case when cast(jsonb_extract_path_text(c.insurance_data, 'member_code') as text) = ''
                then null else cast(jsonb_extract_path_text(c.insurance_data, 'member_code') as text) end as card_number
        , jsonb_extract_path_text(insurance_data, 'additional_member_code') as policy_number
        , jsonb_extract_path_text(c.insurance_data, 'name') as member_name
        , jsonb_extract_path_text(c.insurance_data, 'date_of_birth') as member_dob
        , jsonb_extract_path_text(c.insurance_data, 'gender') as member_gender
        , jsonb_extract_path_text(c.insurance_data, 'member_type') as member_type
        , jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'aggregator', 'code') as aggregator_code
        , jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'aggregator', 'name') as aggregator_name
        , c.patient_user_id as user_id
        , c.patient_profile_id as profile_id
        , jsonb_extract_path_text(c.patient_profile_data, 'name') as name
        , jsonb_extract_path_text(c.patient_profile_data, 'birth_date') as dob
        , jsonb_extract_path_text(c.patient_profile_data, 'gender') as gender 
        , jsonb_extract_path_text(c.patient_profile_data, 'phone') as phone
        , jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'payor', 'code') as payor_code
        , jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'payor', 'name') as payor_name
        , jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'corporate', 'code') as corporate_code
        , jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'corporate', 'name') as corporate_name
        , jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'plan', 'code') as plan_code
        , jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'plan', 'name') as plan_name
        , jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'plan', 'payment_model') as plan_type
        , jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'code') as assigned_plan_code
        , cast(jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'plan', 'benefits', 'GP_CONSULTATION', 'coverage', 'amount') as numeric) as gp_price_override
        , cast(jsonb_extract_path_text(c.insurance_data, 'assigned_plan', 'plan', 'benefits', 'SP_CONSULTATION', 'coverage', 'amount') as numeric) as sp_price_override
        , d.name as doctor_name 
        , dsip.license_number as sip
        , dstr.license_number as str
        , dp.name_id as department_name
        , c.id as consultation_id
        , c.chief_complaints
        , cs.enquiry
        , cs.suggestion 
        , icd_1.icd as icd_1
        , icd_2.icd as icd_2 
        , icd_3.icd as icd_3
        , icd_4.icd as icd_4
        , icd_5.icd as icd_5
        , icd_6.icd as icd_6
        , icd_1.diagnosis as diagnosis_1
        , icd_2.diagnosis as diagnosis_2
        , icd_3.diagnosis as diagnosis_3
        , icd_4.diagnosis as diagnosis_4
        , icd_5.diagnosis as diagnosis_5
        , icd_6.diagnosis as diagnosis_6
        , c.order_id as claim_id
    from 
        consultation as c
        left join 
            (
            select 
                id
                , consult_id
            from 
                prescription p
            where
                date(p.created_at at time zone 'Asia/Jakarta') >= date(current_timestamp at time zone 'Asia/Jakarta' - interval '7' day)
            ) as p on p.consult_id = c.id
        left join doctor as d on d.id = c.doctor_id
        left join doctor_speciality as ds on ds.doctor_id = d.id
        left join (select doctor_id , license_number from doctor_license where type = 'SIP') as dsip on dsip.doctor_id = d.id
        left join (select doctor_id , license_number from doctor_license where type = 'STR') as dstr on dstr.doctor_id = d.id
        left join department as dp on dp.id = ds.department_id
        left join 
            (
            select
                consultation_id
                , enquiry
                , suggestion
            from 
                consult_summary cs
            where 
                date(cs.created_at at time zone 'Asia/Jakarta') >= date(current_timestamp at time zone 'Asia/Jakarta' - interval '7' day)
            ) as cs on cs.consultation_id = c.id
        left join (select * from cd where rn = 1) as icd_1 on icd_1.consultation_id = c.id
        left join (select * from cd where rn = 2) as icd_2 on icd_2.consultation_id = c.id
        left join (select * from cd where rn = 3) as icd_3 on icd_3.consultation_id = c.id
        left join (select * from cd where rn = 4) as icd_4 on icd_4.consultation_id = c.id
        left join (select * from cd where rn = 5) as icd_5 on icd_5.consultation_id = c.id
        left join (select * from cd where rn = 6) as icd_6 on icd_6.consultation_id = c.id
    where 
        c.consultation_status in ('COMPLETED', 'ONGOING', 'CANCELLED', 'WAITING_SUMMARY')
        and jsonb_extract_path_text(c.insurance_data, 'member_code') is not null
        and (date(coalesce(c.end_time at time zone 'Asia/Jakarta', c.est_end_time at time zone 'Asia/Jakarta')) >= date(current_timestamp) - interval '3' day
                )
        and c.start_time is not null
    order by c.id
"""

df_consult = pd.read_sql(q_consult_new, con_med)

df_nah_sehat = pd.read_sql("""
select 
	code
    , nahsehat_payor_code
from 
	payor
""", con_pay)

q_discharge = """

select 
	r.order_id
	, r.claim_id
    , d.discharge_desc
    , (d.discharge_datetime at time zone 'Asia/Jakarta') as discharge_datetime
    , d.discharge_desc as discharge_status
	, d.total_amount
	, d.total_amount_approved 
	, d.total_amount_not_approved 
	, h.error_desc
    , regexp_replace(discharge_printout, E'[\\n\\r]+', ' ', 'g' ) log
from registrations r
left join discharges d on r.claim_id  = d.claim_id 
	left join 
		(
		select
			jsonb_extract_path_text(response_log, 'output', 'txnData', 'eligibilityResponse', 'eligibility', 'clID') as claim_id
		    , jsonb_extract_path_text(response_log, 'output', 'statusCode') as status_code
		    , jsonb_extract_path_text(response_log, 'output', 'txnData', 'dischargeRequestResponse', 'dischargeRequest', 'errorCode') as error_code
		    , jsonb_extract_path_text(response_log, 'output', 'txnData', 'dischargeRequestResponse', 'dischargeRequest', 'errorDesc') as error_desc
		from
			http_gateway_log
		where 
			url = 'https://adcpslite.admedika.co.id:553/admedgateway/services/customerhost'
		 	and jsonb_extract_path_text(request_log, 'input', 'txnData', 'dischargeRequest') is not null 
			and jsonb_extract_path_text(response_log, 'output', 'txnData', 'dischargeRequestResponse', 'dischargeRequest', 'errorDesc') is not null
		) h on h.claim_id = d.claim_id 
order by  r.registration_datetime  desc

"""

df_discharge = pd.read_sql(q_discharge, 
                    con_pay).sort_values(['order_id', 'discharge_datetime'], ascending = False
                            ).drop_duplicates('order_id')

consult_compiled = df_consult.merge(df_nah_sehat,
             left_on = 'payor_code', right_on = 'code', how = 'left')

consult_compiled = consult_compiled.merge(df_discharge, 
            left_on = 'claim_id', right_on = 'order_id', how = 'left', suffixes = ('_x', ''))

consult_compiled = consult_compiled.drop('claim_id_x', axis = 1)


#### Order ####


q_order = """
select
	date(o.created_at at time zone 'Asia/Jakarta') as order_created_date
 	, to_char(o.created_at at time zone 'Asia/Jakarta', 'hh24:mm:ss') as order_created_time
 	, case when o.order_status = 'COMPLETED' then date(o.status_updated_at at time zone 'Asia/Jakarta') end as order_completed_date
 	, case when o.order_status = 'COMPLETED' then to_char(o.status_updated_at at time zone 'Asia/Jakarta', 'hh24:mm:ss') end as order_completed_time
 	, o.order_status
 	, o.order_type as activity_type
 	, o.reference_number
 	, o.id as order_id
 	, jsonb_extract_path_text(o.metadata, 'consult_prescription', 'consult_id')::integer as consult_id
 	, o.payment_method as payment_method
 	, coalesce(coalesce(o.rebooking_benefit_coverage_amount, o.benefit_coverage_amount), 0) as presc_coverage
 	, coalesce(o.voucher_coverage_amount, 0) as presc_voucher
 	, jsonb_extract_path_text(member_snapshot, 'member_code') as card_number
 	, jsonb_extract_path_text(member_snapshot , 'additional_member_code') as policy_number
	, jsonb_extract_path_text(member_snapshot, 'name') as member_name 
	, jsonb_extract_path_text(member_snapshot, 'date_of_birth') as member_dob
	, jsonb_extract_path_text(member_snapshot, 'gender') as member_gender
	, jsonb_extract_path_text(member_snapshot, 'member_type') as member_type
	, jsonb_extract_path_text(member_snapshot, 'assigned_plan', 'aggregator', 'code') as aggregator_code
 	, jsonb_extract_path_text(member_snapshot, 'assigned_plan', 'aggregator', 'name') as aggregator_name
 	, o.user_id 
 	, o.profile_id 
 	, jsonb_extract_path_text(o.metadata, 'user_data', 'name') as name
 	, jsonb_extract_path_text(o.metadata, 'user_data', 'birth_date') as dob
 	, jsonb_extract_path_text(o.metadata, 'user_data', 'gender') as gender
 	, jsonb_extract_path_text(o.metadata, 'user_data', 'phone') as phone
	, jsonb_extract_path_text(o.member_snapshot, 'assigned_plan', 'payor', 'code') as payor_code 
	, jsonb_extract_path_text(o.member_snapshot, 'assigned_plan', 'payor', 'name') as payor_name
	, jsonb_extract_path_text(o.member_snapshot, 'assigned_plan', 'corporate', 'code') as corporate_code 
	, jsonb_extract_path_text(o.member_snapshot, 'assigned_plan', 'corporate', 'name') as corporate_name
	, jsonb_extract_path_text(o.member_snapshot, 'assigned_plan', 'plan', 'code') as plan_code 
	, jsonb_extract_path_text(o.member_snapshot, 'assigned_plan', 'plan', 'name') as plan_name
	, jsonb_extract_path_text(o.member_snapshot, 'assigned_plan', 'plan', 'payment_model') as plan_type
	, jsonb_extract_path_text(o.member_snapshot, 'assigned_plan', 'code') as assigned_plan_code
	, 0 as discount 
	, case 
		when o.rebooking_total is not null 
			then o.rebooking_total+o.rebooking_benefit_coverage_amount-co.rebooking_shipping_fee-co.shipping_fee_markup
		else o.total+o.benefit_coverage_amount-co.shipping_fee-co.shipping_fee_markup+coalesce(o.voucher_coverage_amount, 0)
	end as prescription_fee
	, coalesce(co.rebooking_shipping_fee, co.shipping_fee) + coalesce(co.shipping_fee_markup,0) as deliv_fee
	, coalesce(co.rebooking_shipping_benefit_deduction, co.shipping_benefit_deduction) as deliv_coverage
	, co.shipping_voucher_deduction as deliv_voucher
from
	orders o
	left join commerce_orders co on co.order_id = o.id
where
	o.order_type = 'COMMERCE'
	and o.member_code is not null
	and o.order_status in ('COMPLETED', 'CANCELLED')
	and (o.status_updated_at at time zone 'Asia/Jakarta')::timestamp
		between (now() at time zone 'Asia/Jakarta') - interval '3' day and (now() at time zone 'Asia/Jakarta')

"""


df_order = pd.read_sql(q_order, con_ord)


q_sku = """
select 
			order_id
			, sku_id
			, jsonb_extract_path_text(inventory_snapshot, 'inventory_detail', 'name') as sku_name
			, quantity
			, price
			, quantity*price as total
            , row_number() over(partition by order_id order by sku_id) as rn

		from
			commerce_order_items
		where 
		 (created_at at time zone 'Asia/Jakarta')::timestamp
		between (now() at time zone 'Asia/Jakarta') - interval '3' day and (now() at time zone 'Asia/Jakarta')
"""
df_sku = pd.read_sql(q_sku, con_ord)

df_sku_new = pd.DataFrame({'order_id':[np.nan]})
for i in range(13):
    j = i+1
    df_sku_rn = df_sku[df_sku['rn'] == j].drop('rn', axis = 1)
    df_sku_rn =  df_sku_rn.rename(columns = {'sku_id':'sku_id_'+str(j),
                                'sku_name':'sku_name_'+str(j),
                                'quantity':'quantity_'+str(j),
                                'price':'price_'+str(j),
                                'total':'total_'+str(j)})
    if j == 1:
        df_sku_new = df_sku_rn.merge(df_sku_new, how = 'left', on = 'order_id')
    else :
        df_sku_new = df_sku_new.merge(df_sku_rn, how = 'left', on = 'order_id')


del df_sku
gc.collect()

df_order_compiled = df_order.merge(df_nah_sehat, left_on = 'payor_code', right_on = 'code', how = 'left')
df_order_compiled = df_order_compiled.merge(df_sku_new, on = 'order_id',how = 'left')
df_order_compiled = df_order_compiled.merge(df_discharge, on = 'order_id',how = 'left')




for i in df_order_compiled.filter(like = '_date').columns:
    df_order_compiled[i] = pd.to_datetime(df_order_compiled[i])

pandas_gbq.to_gbq(df_order_compiled, 'ext_source.order_discharge', credentials = credentials, 
                            if_exists = 'replace')

for i in consult_compiled.filter(like = '_date').columns:
    consult_compiled[i] = pd.to_datetime(consult_compiled[i])

pandas_gbq.to_gbq(consult_compiled, 'ext_source.consult_discharge', credentials = credentials, 
                            if_exists = 'replace')