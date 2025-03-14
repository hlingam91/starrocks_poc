LOAD LABEL CRM.s3_loading_20250226_1641
(
DATA INFILE
(
"s3://108532782468--celerdata-zme-audience-segmentation/data/data_0_0_0-2.csv"
)
INTO TABLE USER_PROPS_boomtrain
COLUMNS TERMINATED BY ","
FORMAT AS "CSV"
(
skip_header = 1
trim_space = TRUE
enclose = "\""
)
)
WITH BROKER
(
"aws.s3.use_instance_profile" = "true",
"aws.s3.region"="us-east-1"
)

