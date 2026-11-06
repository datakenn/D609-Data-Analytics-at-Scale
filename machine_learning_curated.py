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

step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted"
).toDF()

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted"
).toDF()

st = step_trainer_trusted.alias("st")
at = accelerometer_trusted.alias("at")

machine_learning_curated = st.join(
    at,
    st["sensorReadingTime"] == at["timestamp"],
    "inner"
).select(
    "st.sensorReadingTime",
    "st.serialNumber",
    "st.distanceFromObject",
    "at.user",
    "at.timestamp",
    "at.x",
    "at.y",
    "at.z"
)

machine_learning_curated.write.mode("overwrite").json("s3://stedi-ken-lakehouse/machine_learning/curated/")

job.commit()
