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

customer_trusted = spark.read.json("s3://stedi-ken-lakehouse/customer/trusted/")
accelerometer_trusted = spark.read.json("s3://stedi-ken-lakehouse/accelerometer/trusted/")

customer_curated = customer_trusted.join(
    accelerometer_trusted,
    customer_trusted.email == accelerometer_trusted.user,
    "inner"
).select(customer_trusted.columns).dropDuplicates()

customer_curated.write.mode("overwrite").json("s3://stedi-ken-lakehouse/customer/curated/")

job.commit()
