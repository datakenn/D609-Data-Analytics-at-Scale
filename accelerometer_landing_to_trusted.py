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

accelerometer_landing = spark.read.json("s3://stedi-ken-lakehouse/accelerometer/landing/")
customer_trusted = spark.read.json("s3://stedi-ken-lakehouse/customer/trusted/")

accelerometer_trusted = accelerometer_landing.join(
    customer_trusted,
    accelerometer_landing.user == customer_trusted.email,
    "inner"
).select(accelerometer_landing.columns)

accelerometer_trusted.write.mode("overwrite").json("s3://stedi-ken-lakehouse/accelerometer/trusted/")

job.commit()
