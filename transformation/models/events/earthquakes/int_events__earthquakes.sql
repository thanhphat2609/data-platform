{{ config(
    materialized = 'external',
    location     = dest_location('events', 'int_earthquakes'),
    format       = 'parquet'
) }}

with earthquakes as (
    select * from {{ ref('stg_events__earthquakes') }}
)

select
    event_id,
    event_time,
    magnitude,
    case
        when magnitude < 2.0 then 'micro'
        when magnitude < 4.0 then 'minor'
        when magnitude < 5.0 then 'light'
        when magnitude < 6.0 then 'moderate'
        when magnitude < 7.0 then 'strong'
        when magnitude < 8.0 then 'major'
        else                      'great'
    end                  as magnitude_category,
    place,
    event_type,
    longitude,
    latitude,
    depth_km,
    status,
    tsunami,
    significance,
    ingestion_date
from earthquakes
