create table user_props (
site_id varchar(100) not null,
bsin varchar(100) not null comment "unique id",
user_id varchar(1000) null,
email varchar(1000) null,
contacts json,
properties json,
last_updated datetime,
replaced_by varchar(100),
email_md5 varchar(100),
sub_site_ids json,
scoped_properties json,
scoped_contacts json,
imported_from json,
merged_bsins json,
consent json,
external_ids json,
unique_client_ids json
)
primary key (site_id, bsin) 
partition by (site_id) 
distributed by hash(bsin);

