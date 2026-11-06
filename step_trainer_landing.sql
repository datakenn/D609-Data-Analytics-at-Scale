CREATE EXTERNAL TABLE IF NOT EXISTS stedi.step_trainer_landing (
  sensorreadingtime BIGINT,
  serialnumber STRING,
  distancefromobject INT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://stedi-ken-lakehouse/step_trainer/landing/'
TBLPROPERTIES ('classification'='json');
