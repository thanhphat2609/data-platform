with source as (
    select * from {{ source('events', 'earthquake_events') }}
)

select
    id              as event_id,
    time            as event_time,
    magnitude,
    place,
    type            as event_type,
    longitude,
    latitude,
    depth_km,
    status,
    tsunami,
    significance,
    ingestion_date
from source
