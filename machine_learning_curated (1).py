import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="step_trainer_trusted"
).toDF()

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="accelerometer_trusted"
).toDF()

st = step_trainer_trusted.alias("st")
at = accelerometer_trusted.alias("at")

machine_learning_curated_df = st.join(
    at, st["sensorReadingTime"] == at["timestamp"], "inner"
).select(
    "st.sensorReadingTime", "st.serialNumber", "st.distanceFromObject",
    "at.user", "at.timestamp", "at.x", "at.y", "at.z"
)

machine_learning_curated_dyf = DynamicFrame.fromDF(machine_learning_curated_df, glueContext, "machine_learning_curated")

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-ken-lakehouse/machine_learning/curated/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("json")
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="machine_learning_curated")
sink.writeFrame(machine_learning_curated_dyf)

job.commit()
