CREATE ROUTINE LOAD CRM.user_prop_load ON USER_PROPS_boomtrain
COLUMNS TERMINATED BY ",",
COLUMNS (bsin, user_id, email, contacts, properties, last_updated, site_id, replaced_by, email_md5, sub_site_ids, scoped_properties, scoped_contacts, imported_from, consent, external_ids, unique_client_ids) 
PROPERTIES
(
"format" = "json",
"jsonpaths" = "[\"$.bsin\",\"$.app_member_id\", \"$.email\",\"$.contacts\",\"$.properties\",\"$.last_updated\",\"$.app_id\",\"$.replaced_by\",\"$.email_md5\",\"$.sub_site_ids\",\"$.s_attributes\",\"$.s_contacts\",\"$.imported_from\",\"$.consent\",\"$.external_ids\",\"$.unique_cli_ids\"]"
)
FROM KAFKA
(
"kafka_broker_list" ="b-1.preprod-msk-asconnect.zmi1o9.c3.kafka.us-east-1.amazonaws.com:9092,b-2.preprod-msk-asconnect.zmi1o9.c3.kafka.us-east-1.amazonaws.com:9092,b-3.preprod-msk-asconnect.zmi1o9.c3.kafka.us-east-1.amazonaws.com:9092",
"kafka_topic" = "as-identity-bsin"
)

