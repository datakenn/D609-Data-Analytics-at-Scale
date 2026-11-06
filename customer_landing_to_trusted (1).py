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

customer_landing = spark.read.json("s3://stedi-ken-lakehouse/customer/landing/")
customer_trusted_df = customer_landing.filter(
    customer_landing.shareWithResearchAsOfDate.isNotNull()
)

customer_trusted_dyf = DynamicFrame.fromDF(customer_trusted_df, glueContext, "customer_trusted")

sink = glueContext.getSink(
    connection_type="s3",
    path="s3://stedi-ken-lakehouse/customer/trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
)
sink.setFormat("json")
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_trusted")
sink.writeFrame(customer_trusted_dyf)

job.commit()
