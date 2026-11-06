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

accelerometer_landing = spark.read.json("s3://stedi-ken-lakehouse/accelerometer/landing/")
customer_trusted = spark.read.json("s3://stedi-ken-lakehouse/customer/trusted/")

accelerometer_trusted_df = accelerometer_landing.join(
    customer_trusted,
    accelerometer_landing.user == customer_trusted.email,
    "inner"
).select(accelerometer_landing.columns)

accelerometer_trusted_dyf = DynamicFrame.fromDF(accelerometer_trusted_df, glueContext, "accelerometer_trusted")

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-ken-lakehouse/accelerometer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("json")
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="accelerometer_trusted")
sink.writeFrame(accelerometer_trusted_dyf)

job.commit()
