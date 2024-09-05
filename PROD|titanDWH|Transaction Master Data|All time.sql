CREATE OR REPLACE table ext_source.discharge_and_invoice as
    with old_mapping as (
    select distinct id as consultation_id, old_consult_id  from `prod_l1_service_medical.consultation` c
    where old_consult_id  is not null and ingest_date < '2024-02-10'

    ), ppu as (

    select cast(coalesce(b.consultation_id, a.consultation_id) as integer) consultation_id ,
    rx_fee_ops_admin rx_total_ops, rx_fee_rx_excess rx_approved_price,
        consult_fee_ops consult_total_ops,consult_fee_consult_excess consult_approved_price,
        coshare, status_discharge, consultation_date, icdx, corporate_name, payor_name  from `ext_source.all_b2b_ppu_trx` a
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
    ppu.* except(consultation_id, icdx), invoice.* except(consultation_id, invoice_date, inv_icdx, insurance_name, invoice_corporate),
    COALESCE(`inv_icdx`, `icdx`) icdx, COALESCE(`invoice_corporate`, `corporate_name`)  invoice_corporate,  insurance_name from ppu
    full join invoice on ppu.consultation_id = invoice.consultation_id;


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
      )

      select a.*,  c.* except(profile_id)
      , f.* except(consultation_id) 
      from 
        consult a
      left join first_trx c on a.profile_id = c.profile_id
      left join funnel f on a.consultation_id = f.consultation_id;
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
      )

      select a.*,  c.* except(profile_id)
      , f.* except(consultation_id) 
      from 
        consult a
      left join first_trx c on a.profile_id = c.profile_id
      left join funnel f on a.consultation_id = f.consultation_id;


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
      select consultation_id, STRING_AGG(icdx, ',') icdx  from disc 
      group by 1 
    )
    , mrg_clean as (

      select a.* except(nahsehat_payor_code), b.* except(consultation_id, insurance_name, invoice_date), 
        lower(nahsehat_payor_code) nahsehat_payor_code, 
          coalesce(lower(trim(insurance_name)), lower(trim(payor_name))) insurance_name,
            date_add(created_at, interval 7 hour) created_at_jkt,
            date_add(updated_at, interval 7 hour) updated_at_jkt,
            case when `member_code` is not null or status_discharge is not null then 1 else 0 end is_b2b_trx_new,
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
         UPPER(trim(coalesce(coalesce(coalesce(coalesce(b.insurance_name_clean, c.insurance_name_clean), 
             d.insurance_missing_1), e.insurance_missing_2), payor_name))) 
             -- else NULL end
              as `Insurance Name`,
             row_number() over(partition by a.consultation_id order by created_at) rn from mrg_clean a
      left join map b on a.insurance_name = b.payor_code
      left join map c on a.nahsehat_payor_code = c.payor_code
      left join map1 d on a.insurance_name = d.insurance_missing_1
      left join map2 e on a.insurance_name = e.payor_name_old
      left join icdx_clean i on i.consultation_id = a.consultation_id

    )

    select * except(`Insurance Name`, rn), case when `Insurance Name` = UPPER('PT Teknologi Pamadya Analitika (MEDITAP)') then UPPER(TRIM(`payor_name`))
           when `Insurance Name` = 'AAA' then UPPER(TRIM(`payor_name`))
           when `Insurance Name` = 'MEDITAP' then UPPER(TRIM(`payor_name`))  else 
`Insurance Name` end as `Insurance Name` from main
where rn = 1;


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
where profile_id is not null and profile_id <> '';


create or replace table ext_source.domo_trx_master_data as
  with cte as (
  select consultation_id, max(is_consult_in_a_day) is_consult_in_a_day, 
      max(multiple_prescription_order) multiple_prescription_order,
      max(multiple_icdx) multiple_icdx  from ext_source.fraud_flagging
      group by 1
  )


  select a.* except(consultation_status, order_status), consultation_status as order_status, b.* except(consultation_id) from `ext_source.client_portal` a
  left join cte b on a.consultation_id = b.consultation_id
;
