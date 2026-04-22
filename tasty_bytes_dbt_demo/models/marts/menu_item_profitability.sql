{{
    config(
        materialized='dynamic_table',
        snowflake_warehouse='COMPUTE_WH',
        target_lag='1 minutes'
    )
}}

select
    menu_item_id,
    menu_item_name,
    truck_brand_name,
    item_category,
    item_subcategory,
    cost_of_goods_usd,
    sale_price_usd,
    sale_price_usd - cost_of_goods_usd as profit_usd,
    round(((sale_price_usd - cost_of_goods_usd) / sale_price_usd) * 100, 2) as profit_margin_pct
from {{ ref('raw_pos_menu') }}



