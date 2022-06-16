
  create or replace  table vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.abs_syn_steering_performance_month_to_date_vw  as 
  select abs_syn.call_date,abs_syn.call_year_month,abs_syn.call_type,
  abs_syn.source_tadig,
    case when initcap(abs_syn.source_operator_name) is null then
  initcap(con_tad.operator_name) else initcap(abs_syn.source_operator_name) 
  end source_operator_name,
   abs_syn.source_group_name,
  case when initcap(abs_syn.source_country_name) is null then initcap(con_tad.country) else initcap(abs_syn.source_country_name) end source_country_name,
  case when upper(abs_syn.source_country_name) is null then upper(con_tad.country) else upper(abs_syn.source_country_name) end home_country,
  abs_syn.destination_tadig,
  abs_syn.target_operator_name,
  initcap(abs_syn.target_group_name) target_group_name,
  initcap(abs_syn.target_country_name) target_country_name,
  upper(abs_syn.target_country_name) visited_country,
  abs_syn.traffic_volume_dst_tadig,
  abs_syn.country_traffic,
  abs_syn.actual_capture_rate,
  abs_syn.target_capture_rate,
  abs_syn.target_based_outbound_costs,
  abs_syn.actual_outbound_costs,abs_syn.cost_delta,
  abs_syn.iot_rate,
  '' discount_period_id,
  '' period_from,
  '' period_to,
  '' discount_model,
  '' commitment_type,
 ''   created_by,
  '' created_date,
  '' last_updated_by,
  '' last_updated_date,
  '' parent_id,
  '' level_number,
  abs_syn.subscriber_type,
  abs_syn.total_target_country_cost_delta,
  abs_syn.total_source_country_cost_delta,abs_syn.total_call_type_cost_delta,abs_syn.total_source_tadig_cost_delta,
  abs_syn.total_dest_tadig_cost_delta,abs_syn.total_target_country_rank,abs_syn.total_source_country_rank,
  abs_syn.total_call_type_rank,abs_syn.total_source_tadig_rank,abs_syn.total_dest_tadig_rank
  from vf-vrs-datahub.vfvrs_dh_lake_bi_tableau_s.abs_syn_steering_performance_month_to_date  abs_syn
  left outer join vf-vrs-datahub.vfvrs_dh_lake_bi_reference_data_s.conversion_tadig_business  con_tad
  on abs_syn.source_tadig = con_tad.tadig;
