import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing"
).toDF()

customer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated"
).toDF()

st = step_trainer_landing.alias("st")
cc = customer_curated.alias("cc")

step_trainer_trusted = st.join(
    cc,
    st["serialNumber"] == cc["serialNumber"],
    "inner"
).select("st.sensorReadingTime", "st.serialNumber", "st.distanceFromObject")

step_trainer_trusted.write.mode("overwrite").json("s3://stedi-ken-lakehouse/step_trainer/trusted/")

job.commit()
