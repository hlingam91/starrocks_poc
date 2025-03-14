COPY INTO @crm.user_props.phoenix_ues/events_boomtrain/ from (
       select event_id,
event_type,
site_id,
bsin,
email,
user_id,
properties,
dt,
etl_time from crm.events.events  where site_id='boomtrain'   and dt > '2024-06-01' limit 2000000 
 
        ) 
        FILE_FORMAT = (TYPE = 'CSV' COMPRESSION = GZIP NULL_IF = ()  FIELD_OPTIONALLY_ENCLOSED_BY = '"') 
        HEADER = TRUE 
        MAX_FILE_SIZE = 10000000, OVERWRITE = TRUE

