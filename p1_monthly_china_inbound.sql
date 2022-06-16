create or replace table vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.p1_monthly_china_inbound as 
select 
count_imsi
, count_data_active
, hmcc
, hmnc
, home_country
, home_operator
, home_tadig
, vmcc
, vmnc
, visited_country
, visited_operator
, visited_tadig
, visited_is_vodafone
, extract_year_month
from vf-vrs-datahub.vfvrs_dh_lake_bi_hist_rawprepared_s.p1_monthly_china_inbound_hist
union all
select count_imsi 
,count_data_active 
,hmcc  
,hmnc  
,home_country  
,home_operator  
,home_tadig  
,vmcc  
,vmnc  
,visited_country  
,visited_operator  
,visited_tadig  
,visited_is_vodafone 
,extract_year_month  
from vf-vrs-datahub.vfvrs_dh_explore_vrs_s_share.p1_monthly_china_inbound
where home_tadig not in ('MLTTL', 'MLTMA');
