with source as (
    select * from {{ source('events', 'openmeteo_hourly') }}
)

select
    datetime,
    date                    as observation_date,
    latitude,
    longitude,
    timezone,
    temperature_2m,
    relative_humidity_2m,
    wind_speed_10m,
    precipitation,
    ingestion_date
from source
