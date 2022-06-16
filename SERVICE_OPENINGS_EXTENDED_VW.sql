
create or replace table vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.tadig_reference as 
with syn_tadig_reference_t01 as 
(  select distinct tadig, syn_operator, syn_country, call_date
from
(select my_pmn_tadig_code as tadig
,my_operator_name as syn_operator
,my_country_name as syn_country
,call_date
,dense_rank() over (partition by my_pmn_tadig_code order by call_date desc) as rank 
from vf-vrs-datahub.vfvrs_dh_lake_bi_syniverse_processed_s.syn_project1_inbound_detailedreport) tmp1
where rank=1

union all 

select distinct their_pmn_tadig_code as tadig, operator_name as syn_operator, their_country_name as syn_country, call_date 
from
(select their_pmn_tadig_code
,case when their_operator_name='' then their_primary_operator_name else their_operator_name end as operator_name
,their_country_name
,call_date
,dense_rank() over (partition by their_pmn_tadig_code order by call_date desc) as rank 
from vf-vrs-datahub.vfvrs_dh_lake_bi_syniverse_processed_s.syn_project1_inbound_detailedreport ) tmp2
where rank=1

union all 
select distinct my_pmn_tadig_code as tadig, my_operator_name as syn_operator, my_country_name as syn_country, call_date
from (select  my_pmn_tadig_code
,my_operator_name
,my_country_name
,call_date
,dense_rank() over (partition by my_pmn_tadig_code order by call_date desc) as rank 
from vf-vrs-datahub.vfvrs_dh_lake_bi_syniverse_processed_s.syn_project1_outbounddetailedreport ) tmp3
where rank=1

union all


select distinct their_pmn_tadig_code as tadig, operator_name as syn_operator, their_country_name as syn_country, call_date 
from
(select their_pmn_tadig_code
,case when their_operator_name='' then their_primary_operator_name else their_operator_name end as operator_name
,their_country_name
,call_date
,dense_rank() over (partition by their_pmn_tadig_code order by call_date desc) as rank
from vf-vrs-datahub.vfvrs_dh_lake_bi_syniverse_processed_s.syn_project1_outbounddetailedreport ) tmp4
where rank = 1
)

, syn_tadig_reference_t02 as 
(select distinct tadig, syn_operator, syn_country, call_date
from (select  tadig 
,syn_operator 
,syn_country
,call_date
,dense_rank() over (partition by tadig order by call_date desc) as rank 
from syn_tadig_reference_t01 ) tmp
where rank=1
)

, tadig_master as ( select distinct tadig from (
select distinct tap_code as tadig from vf-vrs-datahub.vfvrs_dh_lake_bi_abacus_processed_s.abs_operator
union all
select distinct tadig as tadig from syn_tadig_reference_t02 
union all
select distinct tadig_code as tadig from vf-vrs-datahub.vfvrs_dh_lake_bi_salesforce_processed_s.sfc_account
where bdp_active_flag=1
))


select distinct tm.tadig as tadig
,null as syn_region
,syn_tadig.syn_country as syn_country
,syn_tadig.syn_operator as syn_operator
,null as abs_region
,abs_tadig.abs_country as abs_country
,abs_tadig.operator_name as abs_operator
,sfc_tadig.country_region as sfc_region
,sfc_tadig.country as sfc_country
,sfc_tadig.name as sfc_operator
,sfc_tadig.network_name as sfc_network_name
from tadig_master tm
left outer join ( select abs_opr.tap_code as tap_code , abs_opr.operator_name as operator_name, abs_cnt.country_description as abs_country from vf-vrs-datahub.vfvrs_dh_lake_bi_abacus_processed_s.abs_operator abs_opr left outer join vf-vrs-datahub.vfvrs_dh_lake_bi_abacus_processed_s.abs_country abs_cnt on abs_opr.fk_country_id=abs_cnt.country_id ) abs_tadig
on tm.tadig=abs_tadig.tap_code
left outer join syn_tadig_reference_t02 syn_tadig
on tm.tadig=syn_tadig.tadig
left outer join (select country_region, country, name, network_name, tadig_code from  vf-vrs-datahub.vfvrs_dh_lake_bi_salesforce_processed_s.sfc_account  where bdp_active_flag=1) sfc_tadig
on tm.tadig=sfc_tadig.tadig_code
;












create or replace table vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.abs_steering_performance as 

with abs_steering_performance_t01 as (
select 
 crp.capture_rate_performance_id as capture_rate_performance_id
,crp.call_year_month as call_year_month
,crp.call_type as call_type
,crp.src_tadig as source_tadig
,scb.operator_name as source_operator_name
,scb.group_name as source_group_name
,scb.country_name as source_country_name
,scb.region_name as source_region_name
,crp.dst_tadig as destination_tadig
,tcb.operator_name as target_operator_name
,tcb.group_name as target_group_name
,crp.country_description as target_country_name
,tcb.region_name as target_region_name 
,crp.traffic_volume_dst_tadig  as traffic_volume_dst_tadig
,crp.country_traffic as country_traffic
,crp.actual_capture_rate as actual_capture_rate 
,crp.target_capture_rate as target_capture_rate 
,crp.target_based_outbound_costs as target_based_outbound_costs
,crp.actual_outbound_costs as actual_outbound_costs
,(crp.country_traffic*coalesce(iot.curr_out_final_iot, 0)*(crp.TARGET_CAPTURE_RATE - crp.ACTUAL_CAPTURE_RATE)) as cost_delta     
, coalesce(iot.curr_out_final_iot, 0) as iot_rate     
,crp.discount_period_id as discount_period_id
,crp.period_from as period_from
,crp.period_to as period_to
,crp.discount_model as discount_model
,crp.commitment_type as commitment_type
,crp.created_by as created_by
,crp.created_date as created_date
,crp.last_updated_by as last_updated_by
,crp.last_updated_date as last_updated_date
,crp.parent_id as parent_id
,crp.level_number as level_number
from   vf-vrs-datahub.vfvrs_dh_lake_bi_abacus_processed_s.abs_capture_rate_performance  crp
left join  vf-vrs-datahub.vfvrs_dh_lake_bi_abacus_processed_s.abs_iot  iot
on iot.DST_TADIG = crp.DST_TADIG
left join vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.tadig_country_region scb
on trim(crp.src_tadig)=trim(scb.tadig)
left join vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.tadig_country_region tcb
on trim(crp.dst_tadig)=trim(tcb.tadig)
where crp.call_type <> 'TOTAL'
and (crp.dst_tadig is not null
or crp.dst_tadig<>'')
and (crp.src_tadig  is not null
or crp.src_tadig <>'') 
and iot.src_tadig = crp.src_tadig
and iot.call_year_month = crp.call_year_month
and iot.call_type = crp.call_type
and cast(crp.call_year_month as INT64) >= 201801
)

, abs_steering_performance_t02 as (
select 
 capture_rate_performance_id
,call_year_month     
,call_type     
,source_tadig     
,source_operator_name     
,source_group_name     
,source_country_name     
,source_region_name     
,destination_tadig     
,target_operator_name     
,target_group_name     
,target_country_name     
,target_region_name       
,traffic_volume_dst_tadig 
,country_traffic     
,actual_capture_rate     
,target_capture_rate     
,target_based_outbound_costs     
,actual_outbound_costs     
,cost_delta     
,iot_rate     
,discount_period_id     
,period_from     
,period_to     
,discount_model     
,commitment_type
,created_by
,created_date     
,last_updated_by     
,last_updated_date     
,parent_id     
,level_number     
,sum(cost_delta) over (partition by call_year_month,target_country_name) as total_target_country_cost_delta
,sum(cost_delta) over (partition by call_year_month,source_country_name) as total_source_country_cost_delta
,sum(cost_delta) over (partition by call_year_month,call_type) as total_call_type_cost_delta
,sum(cost_delta) over (partition by call_year_month,source_tadig)as total_source_tadig_cost_delta
,sum(cost_delta) over (partition by call_year_month,destination_tadig) as total_dest_tadig_cost_delta   
from abs_steering_performance_t01
)

, abs_steering_performance_t03 as (
select 
 capture_rate_performance_id     
,call_year_month     
,call_type     
,source_tadig     
,source_operator_name     
,source_group_name     
,source_country_name     
,source_region_name     
,destination_tadig     
,target_operator_name     
,target_group_name     
,target_country_name     
,target_region_name     
,traffic_volume_dst_tadig 
,country_traffic     
,actual_capture_rate     
,target_capture_rate     
,target_based_outbound_costs     
,actual_outbound_costs     
,cost_delta     
,iot_rate     
,discount_period_id     
,period_from
,period_to
,discount_model     
,commitment_type
,created_by
,created_date     
,last_updated_by     
,last_updated_date     
,parent_id     
,level_number     
,total_target_country_cost_delta
,total_source_country_cost_delta
,total_call_type_cost_delta
,total_source_tadig_cost_delta
,total_dest_tadig_cost_delta   
,dense_rank() over (partition by call_year_month order by total_target_country_cost_delta ) as total_target_country_rank
,dense_rank() over (partition by call_year_month order by total_source_country_cost_delta) as total_source_country_rank
,dense_rank() over (partition by call_year_month order by total_call_type_cost_delta) as total_call_type_rank
,dense_rank() over (partition by call_year_month order by total_source_tadig_cost_delta) as total_source_tadig_rank
,dense_rank() over (partition by call_year_month order by total_dest_tadig_cost_delta) as total_dest_tadig_rank
from abs_steering_performance_t02)


select 
 capture_rate_performance_id
,call_year_month
,call_type
,source_tadig
,source_operator_name
,source_group_name
,source_country_name
,source_region_name
,destination_tadig
,target_operator_name
,target_group_name
,target_country_name
,target_region_name
,traffic_volume_dst_tadig
,country_traffic    
,actual_capture_rate    
,target_capture_rate    
,target_based_outbound_costs    
,actual_outbound_costs    
,cost_delta    
,iot_rate    
,discount_period_id
,period_from
,period_to
,discount_model
,commitment_type
,created_by 
,created_date
,last_updated_by 
,last_updated_date
,parent_id
,level_number
,total_target_country_cost_delta    
,total_source_country_cost_delta    
,total_call_type_cost_delta    
,total_source_tadig_cost_delta    
,total_dest_tadig_cost_delta    
,total_target_country_rank 
,total_source_country_rank 
,total_call_type_rank 
,total_source_tadig_rank 
,total_dest_tadig_rank 
from  abs_steering_performance_t03
where cast(call_year_month as INT64) <202004

union all 
select 
 capture_rate_performance_id
,call_year_month
,call_type
,source_tadig
,source_operator_name
,source_group_name
,source_country_name
,source_region_name
,destination_tadig
,target_operator_name
,target_group_name
,target_country_name
,target_region_name
,traffic_volume_dst_tadig
,country_traffic    
,actual_capture_rate    
,target_capture_rate    
,target_based_outbound_costs    
,actual_outbound_costs    
,cost_delta    
,iot_rate    
,discount_period_id
,period_from
,period_to
,discount_model
,commitment_type
,created_by 
,created_date
,last_updated_by 
,last_updated_date
,parent_id
,level_number
,total_target_country_cost_delta    
,total_source_country_cost_delta    
,total_call_type_cost_delta    
,total_source_tadig_cost_delta    
,total_dest_tadig_cost_delta    
,total_target_country_rank 
,total_source_country_rank 
,total_call_type_rank 
,total_source_tadig_rank 
,total_dest_tadig_rank 
from  abs_steering_performance_t03
where source_tadig not in ('MLTTL', 'MLTMA') and cast(call_year_month as INT64) >202003
;



create or replace table vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.sfc_service_openings_vw as

with sfc_service_openings_t01 as (
select 
 sfs.name as name
,sfs.hplmn_operator_tadig  home_tadig
,tcrs.operator_name   home_operator
,tcrs.country_name  home_country
,tcrs.region_name home_region
,sfs.vplmn_operator_tadig  visited_tadig
,tcrt.operator_name visited_operator
,tcrt.country_name visited_country
,tcrt.region_name visited_region
,sfs.product product
,sfs.relationship_direction relationship_direction
,sfs.status status
,sfs.so_type type
,sfs.hp_contract_type home_contract_type
,sfs.vp_contract_type visited_contract_type
,max(coalesce(PARSE_DATE("%b %e %Y",(substr(sfs.cll_date, 5, 6) || ' ' || substr(sfs.cll_date,-4)))
, PARSE_DATE("%b %e %Y",(substr(sfs.commercial_migration_date, 5, 6) || ' ' || substr(sfs.commercial_migration_date,-4)))
, PARSE_DATE("%b %e %Y",(substr(sfs.open_date_formula, 5, 6) || ' ' || substr(sfs.open_date_formula,-4))))) as open_date
, PARSE_DATE("%b %e %Y",(substr(vp_asset.shutdown_date, 5, 6) || ' ' || substr(vp_asset.shutdown_date,-4))) as vplmn_shutdown_date
, PARSE_DATE("%b %e %Y",(substr(hp_asset.shutdown_date, 5, 6) || ' ' || substr(hp_asset.shutdown_date,-4))) as hplmn_shutdown_date
from  vf-vrs-datahub.vfvrs_dh_lake_bi_salesforce_processed_s.sfc_footprint_service  sfs
left join vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.tadig_country_region tcrs
on trim(sfs.hplmn_operator_tadig)=trim(tcrs.tadig)
left join vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.tadig_country_region tcrt
on trim(sfs.vplmn_operator_tadig)=trim(tcrt.tadig)
left join vf-vrs-datahub.vfvrs_dh_lake_bi_salesforce_processed_s.sfc_asset vp_asset 
on vp_asset.id=sfs.vplmn_asset_service
left join vf-vrs-datahub.vfvrs_dh_lake_bi_salesforce_processed_s.sfc_asset hp_asset 
on hp_asset.id=sfs.hplmn_account_asset_service
where sfs.bdp_active_flag=1
and vp_asset.bdp_active_flag=1
and hp_asset.bdp_active_flag=1
group by sfs.name 
,sfs.hplmn_operator_tadig
,tcrs.operator_name
,tcrs.country_name  
,tcrs.region_name
,sfs.vplmn_operator_tadig 
,tcrt.operator_name
,tcrt.country_name 
,tcrt.region_name 
,sfs.product 
,sfs.relationship_direction
,sfs.status 
,sfs.so_type 
,sfs.hp_contract_type 
,sfs.vp_contract_type 
,vp_asset.shutdown_date
,hp_asset.shutdown_date
)

, sfc_service_openings as (
select 
name 
, home_tadig 
, home_operator 
, home_country 
, home_region 
, visited_tadig 
, visited_operator
, visited_country
, visited_region
, product
, relationship_direction
, status
, type
, home_contract_type
, visited_contract_type
, open_date
, vplmn_shutdown_date
, hplmn_shutdown_date
from sfc_service_openings_t01 sfs_t01
where
sfs_t01.relationship_direction<>'Bilateral'
or sfs_t01.relationship_direction is null


union all 


select
 name
,home_tadig
,home_operator
,home_country
,home_region
,visited_tadig
,visited_operator
,visited_country
,visited_region
,product
,'Inbound' as relationship_direction
,status
,type
,home_contract_type
,visited_contract_type
,open_date
,vplmn_shutdown_date
,hplmn_shutdown_date
from sfc_service_openings_t01 sfs_t01
where sfs_t01.relationship_direction='Bilateral'

union all 


select
 name
,home_tadig
,home_operator
,home_country
,home_region
,visited_tadig
,visited_operator
,visited_country
,visited_region
,product 
,'Outbound' as relationship_direction
,status
,type
,home_contract_type
,visited_contract_type
,open_date
,vplmn_shutdown_date
,hplmn_shutdown_date
from sfc_service_openings_t01 sfs_t01
where sfs_t01.relationship_direction='Bilateral')


, serv_open as (select name operator_name_id
        ,a.home_tadig
        ,h.sfc_region as home_region
        ,a.visited_tadig
        ,v.sfc_region as visited_region
        ,a.product
        ,a.relationship_direction
        ,a.status
        ,a.type as operator_type
        ,a.home_contract_type
        ,a.visited_contract_type
        ,a.open_date
        ,coalesce(h.sfc_operator,coalesce(h.syn_operator,h.abs_operator)) as home_operator
        ,coalesce(v.sfc_operator,coalesce(v.syn_operator,v.abs_operator)) as visited_operator
        ,coalesce(h.sfc_country,coalesce(h.syn_country,h.abs_country)) as home_country
		,coalesce(v.sfc_country,coalesce(v.syn_country,v.abs_country)) as visited_country
		,a.vplmn_shutdown_date
		,a.hplmn_shutdown_date
		,h.sfc_network_name as home_network_name
		,v.sfc_network_name as tadig_network_name
from sfc_service_openings a  
left outer join vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.tadig_reference h
on a.home_tadig=h.tadig
left outer join vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.tadig_reference v
on a.visited_tadig=v.tadig)

select operator_name_id,home_tadig,home_region,visited_tadig,visited_region,product,relationship_direction,status,operator_type,home_contract_type,visited_contract_type,open_date,home_operator,visited_operator,home_country,visited_country,vplmn_shutdown_date,hplmn_shutdown_date,home_network_name,tadig_network_name from serv_open where home_country is not null and visited_country is not null

;

create or replace table vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.service_openings_extended_vw as

with abs_steering_performance_vw as 
(
select capture_rate_performance_id,call_year_month,call_type,source_tadig,source_operator_name,source_group_name,source_country_name,source_region_name,destination_tadig,target_operator_name,target_group_name,target_country_name,target_region_name,traffic_volume_dst_tadig,country_traffic,actual_capture_rate,target_capture_rate,target_based_outbound_costs,actual_outbound_costs,cost_delta,iot_rate,discount_period_id,period_from,period_to,discount_model,commitment_type,created_by,created_date,last_updated_by,last_updated_date,parent_id,level_number,total_target_country_cost_delta,total_source_country_cost_delta,total_call_type_cost_delta,total_source_tadig_cost_delta,total_dest_tadig_cost_delta,total_target_country_rank,total_source_country_rank,total_call_type_rank,total_source_tadig_rank,total_dest_tadig_rank
from vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.abs_steering_performance
where  destination_tadig is not null 
and source_tadig is not null 
and call_year_month >='201901'
)


, sfc_extended_coverage as (
select 
ext_sfs.id as id
, ext_sfs.asset as product
, ext_sfs.country as country
, ext_sfs.direction as relationship_direction
, ext_sfs.is_main_operator as operator_type
, ext_sfs.lm_operator_tadig as home_tadig
, tcrs.operator_name  home_operator  
, tcrs.country_name  home_country
, tcrs.region_name  home_region
, ext_sfs.lm_operator as lm_operator
, ext_sfs.operator_name as operator_name
, ext_sfs.status as status
, ext_sfs.tadig as visited_tadig
, tcrt.operator_name  visited_operator 
, tcrt.country_name  visited_country
, tcrt.region_name   visited_region
from  vf-vrs-datahub.vfvrs_dh_lake_bi_salesforce_processed_s.sfc_extended_coverage  ext_sfs
left join vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.tadig_country_region tcrs
on trim(ext_sfs.lm_operator_tadig)=trim(tcrs.tadig)
left join vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.tadig_country_region tcrt
on trim(ext_sfs.tadig)=trim(tcrt.tadig)
where ext_sfs.bdp_active_flag=1
and ext_sfs.is_main_operator='false')


, sfc_extended_coverage_vw as (
select sec.id as id ,
sec.home_tadig as home_tadig,
tr.sfc_operator home_operator,
upper(tr.sfc_country) home_country,
upper(tr.sfc_region) home_region,
sec.visited_tadig as visited_tadig,
tr1.sfc_operator visited_operator,
upper(tr1.sfc_country) visited_country,
upper(tr1.sfc_region) visited_region,
sec.product as product,
'Unilateral Outbound' as relationship_direction,
(case sec.STATUS when 'NA' then 'Not Open'
when 'Not Started' then 'Not Open'
when '0' then 'Not Open'
when 'CLL' then 'Progressing'
when 'FD' then 'Progressing'
when 'Impl' then 'Progressing'
when 'Leg HP Done' then 'Progressing'
when 'Pretest' then 'Progressing'
when 'Hold' then 'Progressing'
when 'Awaiting' then 'Progressing'
when 'Live' then 'Open' 
else 'Not Open'
end) as status,
sec.operator_type as operator_type,
null as home_contract_type,
null as visited_contract_type,
null as open_date,
null as target_capture_rate ,
null as steering_settings , 
'Yes' as extended_coverage,
null as vplmn_shutdown_date,
null as hplmn_shutdown_date
from sfc_extended_coverage sec
left join vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.tadig_reference tr on sec.home_tadig=tr.tadig
left join vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.tadig_reference tr1 on sec.visited_tadig=tr1.tadig
)


, sfc_service_openings_vw_latest as (
  select  so.operator_name_id operator_name_id
  ,so.home_tadig home_tadig
  ,so.home_operator home_operator
  ,so.home_country home_country
  ,so.home_region home_region
  ,so.visited_tadig visited_tadig
  ,so.visited_operator visited_operator,
  so.visited_country visited_country
  ,so.visited_region visited_region
  ,so.product product
  ,so.relationship_direction relationship_direction
  ,so.status status
  ,so.operator_type operator_type
  ,so.home_contract_type home_contract_type
  ,so.visited_contract_type visited_contract_type
  ,so.open_date open_date
  ,sp.target_capture_rate target_capture_rate
  ,sp.steering_settings steering_settings
  ,'No' as extended_coverage,
  so.vplmn_shutdown_date vplmn_shutdown_date,
so.hplmn_shutdown_date  hplmn_shutdown_date 
  
from vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.sfc_service_openings_vw so  left join 
(select  distinct case when   source_tadig='NLISR' then 'NLDLT' else 
 source_tadig end as source_tadig,destination_tadig,target_capture_rate ,
  case when target_capture_rate=0 then 'B' 
 when target_capture_rate>=(1/count (case when target_capture_rate>0 then destination_tadig end) 
 over (partition by target_country_name,source_tadig,call_type ,call_year_month)) then 'P' 
 when target_capture_rate<(1/count (case when target_capture_rate>0 then destination_tadig end )
 over (partition by target_country_name,source_tadig,call_type ,call_year_month)) then 'NP' 
 end as steering_settings
 from abs_steering_performance_vw 
 where call_year_month=(select max(call_year_month) from
abs_steering_performance_vw) ) sp on
so.home_tadig=sp.source_tadig and
so.visited_tadig=sp.destination_tadig )



select distinct * from (

  select operator_name_id,home_tadig,home_operator,home_country,home_region,visited_tadig,
  visited_operator,visited_country,visited_region,product,relationship_direction,status,operator_type,
  home_contract_type,visited_contract_type,open_date,target_capture_rate,steering_settings,extended_coverage,
  vplmn_shutdown_date,hplmn_shutdown_date from sfc_service_openings_vw_latest
union all

  select id as OPERATOR_NAME_ID,home_tadig,home_operator,home_country,home_region,visited_tadig,
  visited_operator,visited_country,visited_region,product,relationship_direction,status,operator_type,
  cast(home_contract_type as string),cast(visited_contract_type as string)
  ,SAFE_CAST(null as date) as open_date
  ,target_capture_rate ,safe_cast(null as string) as steering_settings,extended_coverage,
  SAFE_CAST(null as date) as vplmn_shutdown_date,SAFE_CAST(null as date) as hplmn_shutdown_date from sfc_extended_coverage_vw)
;