create or replace table vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.syn_traffic_deviation_vw as 

with syn_traffic_deviation_t01 as
(select call_date
, call_type
, my_pmn_tadig_code
, their_pmn_tadig_code
, their_country_name
, my_country_name
, case when my_pmn_tadig_code='NLDLT' then 
(case when upper(subscriber_type) in ('NLM2M','M2M') then 'M2M'
 when upper(subscriber_type) like '%IOT%' then 'IOT'
 when upper(subscriber_type) like "%POST%" then 'IOT' 
else  'ISR' end) else (case when my_pmn_tadig_code='AAZVF' then 'M2M' else 'IOT' end) end as subscriber_type
, number_of_calls
, real_duration_minutes
, total_real_volume_mb
, total_charges_sdr
from vf-vrs-datahub.vfvrs_dh_lake_bi_syniverse_processed_s.syn_project1_outbounddetailedreport out_dtl_rpt ,
(select DATE_TRUNC(DATE_SUB(max(call_date) , INTERVAL 2 MONTH), MONTH) as max_call_date from vf-vrs-datahub.vfvrs_dh_lake_bi_syniverse_processed_s.syn_project1_outbounddetailedreport) as max_dtl_rpt
where  
out_dtl_rpt.call_date >= max_dtl_rpt.max_call_date
and call_type in ('MOC', 'GPRS'))

, syn_traffic_deviation_t02 as (
select call_date
, call_type
, case when my_pmn_tadig_code='NLDLT' then concat('NL',subscriber_type) else my_pmn_tadig_code end as my_pmn_tadig_code
, their_pmn_tadig_code
, their_country_name
, my_country_name
, subscriber_type
, number_of_calls
, real_duration_minutes
, total_real_volume_mb
, total_charges_sdr
from syn_traffic_deviation_t01 )

, syn_traffic_deviation_t03 as (
select call_date
, call_type
, my_pmn_tadig_code
, their_pmn_tadig_code
, their_country_name
, my_country_name
, subscriber_type
, sum(number_of_calls) as number_of_calls
, sum(real_duration_minutes) as real_duration_minutes
, sum(total_real_volume_mb) as total_real_volume_mb
, sum(total_charges_sdr) as total_charges_sdr
from syn_traffic_deviation_t02
group by call_date, call_type, my_pmn_tadig_code, their_pmn_tadig_code , their_country_name, my_country_name, subscriber_type
)


, syn_max_call_date_daily_calltypetadig as (
select DATE_TRUNC(DATE_SUB(max(call_date) , INTERVAL 2 MONTH), MONTH) as max_call_date from vf-vrs-datahub.vfvrs_dh_lake_bi_syniverse_processed_s.syn_project1_outbounduniqueroamers_daily_visitedtadigcalltype_subscribertype
)

, syn_project1_outbounduniqueroamers_daily_visitedtadigcalltype_subscribertype_t01 as (
select SRC.call_date as call_date
,SRC.my_pmn_tadig_code as my_pmn_tadig_code
,SRC.their_pmn_tadig_code as their_pmn_tadig_code
,SRC.call_type  as call_type
,case when my_pmn_tadig_code='NLDLT' then 
(case when upper(subscriber_type) in ('NLM2M','M2M') then 'M2M'
 when upper(subscriber_type) like '%IOT%' then 'IOT'
 when upper(subscriber_type) like "%POST%" then 'IOT' 
else  'ISR' end) else (case when my_pmn_tadig_code='AAZVF' then 'M2M' else 'IOT' end) end as subscriber_type
,SRC.number_of_unique_roamers  as number_of_unique_roamers
from vf-vrs-datahub.vfvrs_dh_lake_bi_syniverse_processed_s.syn_project1_outbounduniqueroamers_daily_visitedtadigcalltype_subscribertype src,
syn_max_call_date_daily_calltypetadig as max_dtl_rpt
where
src.call_date >= max_dtl_rpt.max_call_date
and src.call_type in ('MOC', 'GPRS'))

, syn_project1_outbounduniqueroamers_daily_visitedtadigcalltype_subscribertype_t02 as (
select call_date 
, case when my_pmn_tadig_code='NLDLT' then concat('NL',subscriber_type) else my_pmn_tadig_code end as my_pmn_tadig_code
,their_pmn_tadig_code
,call_type 
,subscriber_type
,number_of_unique_roamers 
from syn_project1_outbounduniqueroamers_daily_visitedtadigcalltype_subscribertype_t01)


, syn_project1_outbounduniqueroamers_daily_visitedtadigcalltype_subscribertype_t03 as (
select call_date 
, my_pmn_tadig_code
,their_pmn_tadig_code
,call_type 
,subscriber_type
,sum(number_of_unique_roamers) as number_of_Unique_Roamers
from syn_project1_outbounduniqueroamers_daily_visitedtadigcalltype_subscribertype_t02
group by call_date, my_pmn_tadig_code, their_pmn_tadig_code, call_type, subscriber_type )



, syn_traffic_deviation as (
select traf_devi.call_date as call_date
, traf_devi.call_type as call_type
, traf_devi.my_pmn_tadig_code as my_pmn_tadig_code
, traf_devi.their_pmn_tadig_code as their_pmn_tadig_code
, traf_devi.their_country_name as their_country_name
, traf_devi.my_country_name as my_country_name
, traf_devi.subscriber_type as subscriber_type
, traf_devi.number_of_calls as number_of_calls
, traf_devi.real_duration_minutes as real_duration_minutes
, traf_devi.total_real_volume_mb as total_real_volume_mb
, traf_devi.total_charges_sdr as total_charges_sdr
, case when traf_devi.call_type='MOC' then safe_divide(traf_devi.total_charges_sdr,traf_devi.real_duration_minutes) else safe_divide(traf_devi.total_charges_sdr,traf_devi.total_real_volume_mb) end as tap_rate_sdr
, uniq_roamer.number_of_unique_roamers  as number_of_unique_roamers
from syn_traffic_deviation_t03  traf_devi
left join syn_project1_outbounduniqueroamers_daily_visitedtadigcalltype_subscribertype_t03 uniq_roamer
on 
traf_devi.call_date = uniq_roamer.call_date
and traf_devi.my_pmn_tadig_code = uniq_roamer.my_pmn_tadig_code
and traf_devi.their_pmn_tadig_code = uniq_roamer.their_pmn_tadig_code
and traf_devi.call_type = uniq_roamer.call_type
and traf_devi.subscriber_type=uniq_roamer.subscriber_type )

  
select call_date
,a.call_type as call_type
,a.my_pmn_tadig_code as my_pmn_tadig_code
,b.operator_name as home_operator 
,a.my_country_name as my_country_name
, their_pmn_tadig_code
,c.operator_name as visited_operator
,a.their_country_name
,case when a.my_pmn_tadig_code in ('AAZVF','MLTMA') or a.subscriber_type='M2M'  then 'M2M'
  when a.subscriber_type='ISR' then 'ISR'
  else 'IOT' 
  end as traffic_type
, a.subscriber_type as subscriber_type
, a.number_of_calls as number_of_calls,
  a.real_duration_minutes as real_duration_minutes ,
  a.total_real_volume_mb as total_real_volume_mb,
  a.total_charges_sdr as total_charges_sdr,
  a.tap_rate_sdr as tap_rate_sdr,
  a.number_of_unique_roamers as number_of_unique_roamers 
 from syn_traffic_deviation   a  
  left outer join vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.conversion_tadig_business b
on a.my_pmn_tadig_code=b.tadig
left outer join vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.conversion_tadig_business c
on a.their_pmn_tadig_code=c.tadig;





