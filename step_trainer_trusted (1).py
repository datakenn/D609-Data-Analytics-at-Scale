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

step_trainer_landing = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="step_trainer_landing"
).toDF()

customer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi", table_name="customer_curated"
).toDF()

st = step_trainer_landing.alias("st")
cc = customer_curated.alias("cc")

step_trainer_trusted_df = st.join(
    cc, st["serialNumber"] == cc["serialNumber"], "inner"
).select("st.sensorReadingTime", "st.serialNumber", "st.distanceFromObject")

step_trainer_trusted_dyf = DynamicFrame.fromDF(step_trainer_trusted_df, glueContext, "step_trainer_trusted")

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-ken-lakehouse/step_trainer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("json")
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="step_trainer_trusted")
sink.writeFrame(step_trainer_trusted_dyf)

job.commit()
