create or replace table vfvrs_dh_lake_bi_tableau_s.vw_syn_impact_inbound_detailed_report_sdr_impact_per_alt1_prev as
select * from vfvrs_dh_lake_bi_tableau_s.vw_syn_impact_inbound_detailed_report_sdr_impact_per_alt1;

create or replace table vfvrs_dh_lake_bi_tableau_s.vw_syn_impact_inbound_detailed_report_sdr_impact_per_alt1 as 
with vw_syn_impact_inbound_detailed_report_sdr as (
select call_date, my_pmn_tadig_code, their_pmn_tadig_code, call_type, sum(total_charged_volume_mb) total_charged_volume_mb,  sum(total_charges_sdr) total_charges_sdr
from vf-vrs-datahub.vfvrs_dh_lake_bi_syniverse_processed_s.syn_project1_inbound_detailedreport
where call_date > current_date-100
and my_pmn_tadig_code not in ('AUSVF','EGYMS','QATB1','MLTTL')
and their_pmn_tadig_code not in ('ALBVF','CODVC','CZECM','DEUD2','ESPAT','GBRVF','GHAGT','GRCPF','HUNVR','IRLEC','ITAOM','LSOVL','MLTTL','MOZVC','NLDLT','NZLBS','PRTTL','ROMMF','TURTS','TZAVC','ZAFVC')
and call_type='GPRS' and concat(my_pmn_tadig_code,their_pmn_tadig_code) not in ('NZLBSNZLNH','ITAOMITAFS','DEUD2DEU42')
group by call_date, my_pmn_tadig_code, their_pmn_tadig_code, call_type
),

tbl_vw_syn_impact_inbound_detailed_report_sdr as (
select
syn_ib_dtl_rpt.call_date call_date,  
syn_ib_dtl_rpt.my_pmn_tadig_code my_pmn_tadig_code, 
syn_ib_dtl_rpt.their_pmn_tadig_code their_pmn_tadig_code, 
syn_ib_dtl_rpt.call_type call_type, syn_ib_dtl_rpt.total_charged_volume_mb total_charged_volume_mb,
syn_ib_dtl_rpt.total_charges_sdr total_charges_sdr, 
(syn_ib_dtl_rpt.total_charges_sdr * syn_fx.sdr_to_local_currency_exchange_rate) total_charges_eur,
case when total_charged_volume_mb is null then 0
else safe_divide((syn_ib_dtl_rpt.total_charges_sdr * syn_fx.sdr_to_local_currency_exchange_rate), total_charged_volume_mb) end iot_rate_eur        
from vw_syn_impact_inbound_detailed_report_sdr syn_ib_dtl_rpt left outer join ( select * from vf-vrs-datahub.vfvrs_dh_lake_bi_syniverse_processed_s.syn_project1_fxrates where currency_code='EUR') syn_fx
on syn_ib_dtl_rpt.call_date >=  syn_fx.effective_date and syn_ib_dtl_rpt.call_date <= syn_fx.end_date
),

vw_syn_impact_inbound_detailed_report_sdr_impact_per as (
select  in_dtl_rpt.my_pmn_tadig_code as my_pmn_tadig_code
,in_dtl_rpt.their_pmn_tadig_code as their_pmn_tadig_code
,in_dtl_rpt.call_type as call_type
,in_dtl_rpt.total_charged_volume_mb as total_charged_volume_mb
,in_dtl_rpt.total_charges_sdr as total_charges_sdr
,in_dtl_rpt.total_charges_eur as total_charges_eur
,iot_30.total_charges_eur total_charges_eur_30
,in_dtl_rpt.call_date as call_date
,iot_30.call_date  call_date_30 
,in_dtl_rpt.iot_rate_eur as iot_rate_eur
,iot_30.iot_rate_eur iot_rate_eur_30
,case when iot_30.iot_rate_eur = 0 then 0
            else (safe_divide((in_dtl_rpt.iot_rate_eur - iot_30.iot_rate_eur), iot_30.iot_rate_eur) * 100 ) end as percentage
, (in_dtl_rpt.iot_rate_eur - iot_30.iot_rate_eur) * in_dtl_rpt.total_charged_volume_mb  as impact

from tbl_vw_syn_impact_inbound_detailed_report_sdr  in_dtl_rpt
left outer join tbl_vw_syn_impact_inbound_detailed_report_sdr  iot_30
on in_dtl_rpt.call_date =  date_add(iot_30.call_date, interval 1 month)
and in_dtl_rpt.my_pmn_tadig_code=iot_30.my_pmn_tadig_code
and in_dtl_rpt.their_pmn_tadig_code=iot_30.their_pmn_tadig_code
and in_dtl_rpt.call_type=iot_30.call_type
where  in_dtl_rpt.call_date > current_date-35)

select my_pmn_tadig_code, their_pmn_tadig_code, call_type, total_charged_volume_mb, total_charges_sdr, total_charges_eur, total_charges_eur_30, call_date, call_date_30, iot_rate_eur, iot_rate_eur_30, percentage, '1' latest_flag from 
(select * from vw_syn_impact_inbound_detailed_report_sdr_impact_per
where ( percentage > 20  or iot_rate_eur > 0.90)
and  total_charges_eur > 10000 and total_charged_volume_mb > 150)  alt1
where not exists (select 1 
from vfvrs_dh_lake_bi_tableau_s.vw_syn_impact_inbound_detailed_report_sdr_impact_per_alt1_prev prev
where alt1.call_date=prev.call_date
and alt1.call_date_30=prev.CALL_DATE_30
and alt1.my_pmn_tadig_code=prev.MY_PMN_TADIG_CODE
and alt1.their_pmn_tadig_code=prev.THEIR_PMN_TADIG_CODE
and alt1.call_type=prev.CALL_TYPE)
union all
select  my_pmn_tadig_code, their_pmn_tadig_code, call_type, total_charged_volume_mb, total_charges_sdr, total_charges_eur, total_charges_eur_30, call_date, call_date_30, iot_rate_eur, iot_rate_eur_30, percentage, '0' latest_flag
from vfvrs_dh_lake_bi_tableau_s.vw_syn_impact_inbound_detailed_report_sdr_impact_per_alt1_prev;
