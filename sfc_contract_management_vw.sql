

create or replace table vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.sfc_contract_management_vw as 
with sfc_negotiator_details as (
select
 id
,contract_name
,negotiator_email
,coalesce(CONCAT(initcap(split(split(negotiator_email,'@')[OFFSET(0)],'.')[OFFSET(0)]),
         ' ',initcap(split(split(negotiator_email,'@')[OFFSET(0)],'.')[OFFSET(1)])),'') as negotiator
,contract_category
,max(name) as employee_num
from vf-vrs-datahub.vfvrs_dh_lake_bi_salesforce_processed_s.sfc_e2e_ecas
where bdp_active_flag=1
group by
id,
contract_name,
negotiator_email,
contract_category)

, sfc_contract_management as (
select 
sec.id as id
,sec.name as deal_id
,sec.contract_name as contract_name
,sec.risk as risk
,sec.contract_status as contract_status
,sec.report_status as report_status
,parse_date("%b %e %Y",(substr(effective_date, 5, 6) || ' ' || substr(effective_date,-4))) as effective_date
,snd.negotiator negotiator 
,sec.contract_type contract_type
,snd.contract_category contract_category
,fyr.fy as fiscal_year
,sec.lawyer lawyer
,parse_date("%b %e %Y",(substr(start_date, 5, 6) || ' ' || substr(start_date,-4))) as start_date
,sec.ecas ecas
,sec.entity_name entity_name
,sec.contract_doc_link contract_doc_link
,sec.contract_document contract_document
,sec.amendment_to_be_reported amendment_to_be_reported
,sec.amendment_type  amendment_type
,sec.is_split_contract is_split_contract
,sec.main_contract main_contract
,sec.isr_deal isr_deal
,sec.discounted_charges_ib_revenue discounted_charges_ib_revenue
,sec.discounted_charges_ob_revenue discounted_charges_ob_revenue
,sec.contract_manager contract_manager
,sec.action action
,sec.iot_contract_index iot_contract_index
,sec.contract_complexity contract_complexity
,sec.commercial_manager commercial_manager
,sec.second_contract_party second_contract_party
from vf-vrs-datahub.vfvrs_dh_lake_bi_salesforce_processed_s.sfc_e2e_contract sec
left join sfc_negotiator_details snd
on sec.ecas=snd.id
cross join vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.fiscal_year fyr
where parse_date("%b %e %Y",(substr(start_date, 5, 6) || ' ' || substr(start_date,-4))) >=fyr.fy_start_date
and  parse_date("%b %e %Y",(substr(start_date, 5, 6) || ' ' || substr(start_date,-4))) <=fyr.fy_end_date
and sec.bdp_active_flag=1)


  select scm.deal_id as deal_id
  ,scm.contract_name contract_name
  ,scm.risk risk
  ,scm.contract_status contract_status
  ,scm.report_status report_status
  ,scm.effective_date effective_date
  ,scm.negotiator negotiator
  ,scm.contract_type contract_type
  ,scm.contract_category contract_category
  ,scm.fiscal_year fiscal_year
  ,scm.lawyer lawyer
 ,scm.start_date start_date
 ,scm.ecas ecas
 ,scm.entity_name entity_name
  ,scm.contract_doc_link contract_doc_link
  ,scm.contract_document contract_document
  , scm.amendment_to_be_reported amendment_to_be_reported
  ,scm.amendment_type amendment_type
  , scm.is_split_contract is_split_contract
  , scm.main_contract main_contract
  , scm.isr_deal isr_deal
  ,scm.discounted_charges_ib_revenue discounted_charges_ib_revenue
  ,scm.discounted_charges_ob_revenue discounted_charges_ob_revenue
  ,scm.contract_manager contract_manager
  ,scm.action action
  ,a.email_id email_id
  ,a.name name
  ,scm.contract_complexity contract_complexity
  ,b.name commercial_manager
  from sfc_contract_management scm
  left join vfvrs_dh_lake_bi_reference_data_s.sfc_user_static a
  on scm.contract_manager = a.id 
   left join vfvrs_dh_lake_bi_reference_data_s.sfc_user_static b
   on scm.commercial_manager = b.id 
  where scm.iot_contract_index not in (select deal_index from vf-vrs-datahub.vfvrs_dh_lake_bi_salesforce_processed_s.sfc_one_contract_deleted);
