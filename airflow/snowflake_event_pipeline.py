from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="snowflake_event_pipeline",
    start_date=datetime(2026, 2, 28),
    schedule="@hourly",
    catchup=False,
    tags=["snowflake", "learning"],
) as dag:
    run_raw_load_sql = SQLExecuteQueryOperator(
        task_id="run_raw_load_sql",
        conn_id="snowflake_default",
        sql="""
        use warehouse LEARN_WH;
        use database LEARN_DB;
        use schema RAW;

        alter pipe RAW.EVENTS_PIPE refresh;
        """,
    )

    run_transform_sql = SQLExecuteQueryOperator(
        task_id="run_transform_sql",
        conn_id="snowflake_default",
        sql="""
        use warehouse LEARN_WH;
        use database LEARN_DB;
        use schema MART;

        merge into MART.FACT_PURCHASE_EVENTS tgt
        using (
          select
            s.raw:event_id::string as event_id,
            s.raw:user_id::string as user_id,
            to_timestamp_ntz(s.raw:event_time::string) as event_time,
            item.value:sku::string as sku,
            item.value:product_name::string as product_name,
            item.value:category::string as category,
            item.value:qty::number as qty,
            item.value:price::number(10,2) as price,
            item.value:qty::number * item.value:price::number(10,2) as line_amount,
            s.src_filename
          from RAW.RAW_EVENTS_STREAM s,
          lateral flatten(input => s.raw:items) item
          where s.metadata$action = 'INSERT'
        ) src
        on tgt.event_id = src.event_id
        and tgt.sku = src.sku
        when matched then update set
          tgt.user_id = src.user_id,
          tgt.event_time = src.event_time,
          tgt.product_name = src.product_name,
          tgt.category = src.category,
          tgt.qty = src.qty,
          tgt.price = src.price,
          tgt.line_amount = src.line_amount,
          tgt.src_filename = src.src_filename
        when not matched then insert (
          event_id,
          user_id,
          event_time,
          sku,
          product_name,
          category,
          qty,
          price,
          line_amount,
          src_filename
        ) values (
          src.event_id,
          src.user_id,
          src.event_time,
          src.sku,
          src.product_name,
          src.category,
          src.qty,
          src.price,
          src.line_amount,
          src.src_filename
        );
        """,
    )

    run_quality_check = SQLExecuteQueryOperator(
        task_id="run_quality_check",
        conn_id="snowflake_default",
        sql="""
        select count(*) as row_count
        from LEARN_DB.MART.FACT_PURCHASE_EVENTS;
        """,
    )

    run_raw_load_sql >> run_transform_sql >> run_quality_check
