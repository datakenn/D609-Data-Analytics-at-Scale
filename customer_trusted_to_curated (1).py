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

customer_trusted = spark.read.json("s3://stedi-ken-lakehouse/customer/trusted/")
accelerometer_trusted = spark.read.json("s3://stedi-ken-lakehouse/accelerometer/trusted/")

customer_curated_df = customer_trusted.join(
    accelerometer_trusted,
    customer_trusted.email == accelerometer_trusted.user,
    "inner"
).select(customer_trusted.columns).dropDuplicates()

customer_curated_dyf = DynamicFrame.fromDF(customer_curated_df, glueContext, "customer_curated")

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-ken-lakehouse/customer/curated/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("json")
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_curated")
sink.writeFrame(customer_curated_dyf)

job.commit()
