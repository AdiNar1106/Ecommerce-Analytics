{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}

WITH source as (
    SELECT
        * 
    FROM 
        {{ source('raw', 'events') }}
    WHERE 1=1 

    {% if var('through_date', none) %}
    and _export_date <= date '{{ var("through_date") }}'
    {% endif %}

    {%if is_incremental()%}
    and _export_date >= (
        select max(_export_date) - {{var('through_date', 1)}}
        from {{this}}
    )
    {% endif %}

),

column_transforms as (
    SELECT 
        event_id, 
        cast(user_id as bigint) as user_id, 
        nullif(trim(user_session), '') as session_id,
        cast(product_id as bigint) as product_id,
        cast(category_id as bigint) as category_id,
        lower(trim(event_type)) as event_type,
        cast(replace(event_time,  ' UTC', '') as timestamp) as event_at,
        cast(replace(received_at, ' UTC', '') as timestamp) as received_at,
        nullif(trim(category_code), '')                  as category_code,
        nullif(split_part(category_code, '.', 1), '')    as category_l1,
        nullif(split_part(category_code, '.', 2), '')    as category_l2,
        nullif(split_part(category_code, '.', 3), '')    as category_l3,
        nullif(lower(trim(brand)), '')                   as brand,
        cast(price as decimal(10, 2))                    as price,

        -- lineage (carried through untouched)
        _source_file,
        _export_date,
        _extracted_at
        
    FROM 
        source
),

deduplicated_events as (
    SELECT *, 
    row_number() over (partition by event_id order by received_at, _source_file asc) as rnk
    FROM 
    column_transforms
    qualify 
    row_number() over (partition by event_id order by received_at, _source_file asc) = 1
),
final as (
    SELECT 
        event_id,
        user_id,
        session_id,
        product_id,
        category_id,
        event_type,
        event_at,
        received_at,
        category_code,
        category_l1,
        category_l2,
        category_l3,
        brand,
        price,

        -- flags, not filters
        price <= 0                                       as is_zero_price,
        cast(received_at as date) > cast(event_at as date) as is_late_arrival,

        _source_file,
        _export_date,
        _extracted_at
    FROM 
    deduplicated_events
)
select * from final