select * from udl.newsletter;
select * from udl.newsletter_hist;

select * from dbt_udl.wrk_newsletter;
select * from dbt_udl.snap_newsletter;

select * from dbt_execution_run_stats.dbt_model_log;

select * from dbt_execution_run_stats.dbt_run_log;


select * from shared_services_staging.vw_enl_newsletter;

select * from shared_services_staging.enl_newsletter
where id in (12324,	12322)
;


select * from config.archive_entity_mapping;


select * from common_tenant_dev.shared_services_staging.enl_newsletter_archive order by id desc;




ALTER USER devraj_gurjar SET EMAIL = 'devraj.gurjar@simpplr.com';
