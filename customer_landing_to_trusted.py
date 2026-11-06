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

customer_landing = spark.read.json("s3://stedi-ken-lakehouse/customer/landing/")

customer_trusted = customer_landing.filter(
    customer_landing.shareWithResearchAsOfDate.isNotNull()
)

customer_trusted.write.mode("overwrite").json("s3://stedi-ken-lakehouse/customer/trusted/")

job.commit()
