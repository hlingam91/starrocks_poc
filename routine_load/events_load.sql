CREATE ROUTINE LOAD CRM.event_load ON EVENTS_BOOMTRAIN
COLUMNS TERMINATED BY ",",
COLUMNS (event_id, event_type, site_id, bsin, email, user_id, properties, dt, etl_time )
PROPERTIES
(
    "format" = "json",
    "jsonpaths" = "[\"$.event_id\",\"$.event_type\", \"$.site_id\",\"$.bsin\",\"$.identity.email\",\"$.identity.user_id\",\"$.properties\",\"$.timestamp\",\"$.metadata.receive_timestamp\"]"
 )
FROM KAFKA
(
    "kafka_broker_list" ="b-1.preprodmskevents2.w7ke8n.c17.kafka.us-east-1.amazonaws.com:9092,b-2.preprodmskevents2.w7ke8n.c17.kafka.us-east-1.amazonaws.com:9092,b-3.preprodmskevents2.w7ke8n.c17.kafka.us-east-1.amazonaws.com:9092",
    "kafka_topic" = "events_processed"
);

