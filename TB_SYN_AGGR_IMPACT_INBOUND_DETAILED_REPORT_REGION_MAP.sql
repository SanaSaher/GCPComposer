create or replace TABLE vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.tb_syn_aggr_impact_inbound_detailed_report_region_map as 
with max_date_call_date as  (select 
max( call_date) as cdate
from vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.syn_aggr_impact_inbound_detailed_report where call_date < current_date -6 )
, abs_iot_rate_rank_t01 as (
select dst_tadig
, src_tadig
, call_year_month
, call_type
, curr_in_final_iot
, curr_out_final_iot
from vfvrs_dh_lake_bi_abacus_processed_s.abs_iot 
where traffic_type='Actual'
and call_year_month<'202004'

union all

select dst_tadig
, src_tadig
, call_year_month
, call_type
, curr_in_final_iot
, curr_out_final_iot
from vfvrs_dh_lake_bi_abacus_processed_s.abs_iot 
where traffic_type='Actual'
and src_tadig not in ('MLTTL', 'MLTMA') and call_year_month>'202003')

, abs_iot_rate_rank as 
( select dst_tadig as destination_tadig
, src_tadig as source_tadig
, call_year_month
, call_type
, curr_in_final_iot
, curr_out_final_iot
, dense_rank() over (partition by dst_tadig, src_tadig, call_type order by call_year_month desc) as rnk 
from abs_iot_rate_rank_t01)

,syn_aggr_impact_inbound_detailed_report_map_level1_temp as(
 select a.call_date,
a.call_month,
a.call_year,
a.my_pmn_tadig_code,
initcap(b.country) as my_pmn_country, 
a.my_operator_name,
a.their_pmn_tadig_code,
initcap(a.their_country_name) as their_pmn_country,
case when b.intragroup_eligible is true and  c.intragroup_eligible is true 
		then 'Yes' else 'No' end as intragroup,
case when (upper(b.traffic_type)='M2M' or upper(c.traffic_type)='M2M')             then 'M2M' 
				   when (upper(b.traffic_type)='ISR' or upper(c.traffic_type)='ISR')   then 'ISR' 
	   when substr(a.my_pmn_tadig_code,0,3) = substr(a.their_pmn_tadig_code,0,3) and a.their_country_name=a.serving_country_name then 'NR'
				  when (upper(b.traffic_type)='OTHER' or upper(c.traffic_type)='OTHER') then 'OTHER'
				  else 'IOT' end as  traffic_type,
																			
case when (a.call_date)>(select cdate from max_date_call_date)-28 then 'Y' 
	when (a.call_date)>=(select cdate from max_date_call_date)-393 and (a.call_date)<=(select cdate from max_date_call_date)-366 then 'Y'
	else 'N' end  as last_28_days,
		
initcap(a.serving_country_name) serving_country_name,
a.serving_network,
a.subscriber_type,
a.call_type,
a.number_of_calls,
a.total_real_volume_mb,
a.real_duration_minutes,
a.total_charged_volume_mb,
a.charged_duration_minutes,
a.total_charges_local_currency,
case when (upper(b.traffic_type)='ISR' or upper(c.traffic_type)='ISR') then null
		else ( case when iot.curr_in_final_iot is null then ( case when cast(iot_rev_null.call_year_month as INT64) < (extract(year from (date_sub(call_date, interval 3 month ))) * 100 + (extract(month from (date_sub(call_date, interval 3 month )))))  
					then null else iot_rev_null.curr_in_final_iot end ) else iot.curr_in_final_iot end) end as iot_rate, 
					
case when (upper(b.traffic_type)='ISR' or upper(c.traffic_type)='ISR') then null
		else ( case when iot.curr_in_final_iot is null then ( case when cast(iot_rev_null.call_year_month as INT64) < (extract(year from (date_sub(call_date, interval 3 month ))) * 100 + (extract(month from (date_sub(call_date, interval 3 month ))))) 
					then null else iot_rev_null.call_year_month end ) else iot.call_year_month end) end as iot_call_year_month,


from  vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.syn_aggr_impact_inbound_detailed_report a

left outer join vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.conversion_tadig_business b
on a.my_pmn_tadig_code=b.tadig

left outer join  vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.conversion_tadig_business c
on a.their_pmn_tadig_code=c.tadig

left outer join abs_iot_rate_rank iot
on a.their_pmn_tadig_code = iot.destination_tadig
and a.my_pmn_tadig_code = iot.source_tadig
and a.call_month=cast(iot.call_year_month as INT64)
and a.call_type=iot.call_type

left outer join abs_iot_rate_rank iot_rev_null
on a.their_pmn_tadig_code = iot_rev_null.destination_tadig
and a.my_pmn_tadig_code = iot_rev_null.source_tadig
and a.call_type=iot_rev_null.call_type
and iot_rev_null.rnk=1

where  
 a.call_date>='2019-02-01' and   call_date < current_date -6)
 ,
 vw_syn_aggr_impact_inbound_detailed_report_map_temp as
 (select 
call_date                   
, call_month                  
, call_year                   
, my_pmn_tadig_code           
, my_pmn_country              
, my_operator_name            
, their_pmn_tadig_code        
, their_pmn_country           
, intragroup                  
, traffic_type                
, last_28_days                
, serving_country_name        
, serving_network             
, subscriber_type             
, call_type                   
, number_of_calls             
, total_real_volume_mb        
, real_duration_minutes       
, total_charged_volume_mb     
, charged_duration_minutes    
, total_charges_local_currency
, iot_rate
, iot_call_year_month
, case  when call_type='MOC'  then (real_duration_minutes * iot_rate)
          when  call_type='GPRS' then (total_real_volume_mb * iot_rate)
          when call_type='SMS-MT' then 0
          when call_type='SMS-MO' then (number_of_calls * iot_rate)
          when call_type='MTC' then (real_duration_minutes * iot_rate)
          when call_type='SS'  then null
          else null end as estimated_revenue 
                 
from
syn_aggr_impact_inbound_detailed_report_map_level1_temp)
,
syn_aggr_impact_inbound_detailed_report_map as(
select call_date, call_month, call_year, my_pmn_tadig_code, my_pmn_country, my_operator_name, their_pmn_tadig_code, their_pmn_country, intragroup, traffic_type, last_28_days, serving_country_name, serving_network, subscriber_type, call_type, number_of_calls, total_real_volume_mb, real_duration_minutes, total_charged_volume_mb, charged_duration_minutes, total_charges_local_currency, iot_rate, iot_call_year_month, estimated_revenue 
	from vw_syn_aggr_impact_inbound_detailed_report_map_temp
	)

 select yr.call_date, yr.call_month,                       
yr.call_year,yr.my_pmn_tadig_code,
yr.my_pmn_country,                    
yr.my_operator_name,                  
yr.their_pmn_tadig_code,              
yr.their_pmn_country,                 
yr.intragroup,                        
yr.traffic_type ,                     
yr.last_28_days,                      
yr.serving_country_name ,             
yr.serving_network   ,                
yr.subscriber_type ,                  
yr.call_type     ,                    
yr.number_of_calls   ,                
yr.total_real_volume_mb    ,          
yr.real_duration_minutes   ,          
yr.total_charged_volume_mb     ,      
yr.charged_duration_minutes    ,      
yr.total_charges_local_currency  ,    
yr.iot_rate      ,                    
yr.iot_call_year_month  ,             
yr.estimated_revenue     ,            

ry.region from syn_aggr_impact_inbound_detailed_report_map yr

left join  vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.conversion_tadig_business   ry
    on yr.their_pmn_tadig_code=ry.tadig;


