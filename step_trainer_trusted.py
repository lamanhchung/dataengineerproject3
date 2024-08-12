import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer curated
Customercurated_node1723421454692 = glueContext.create_dynamic_frame.from_catalog(database="spark_datalake", table_name="customer_curated", transformation_ctx="Customercurated_node1723421454692")

# Script generated for node Step trainer landing
Steptrainerlanding_node1723421426087 = glueContext.create_dynamic_frame.from_catalog(database="spark_datalake", table_name="step_trainer_landing", transformation_ctx="Steptrainerlanding_node1723421426087")

# Script generated for node SQL Query
SqlQuery0 = '''
select s.sensorreadingtime, s.serialnumber, s.distancefromobject from s INNER JOIN c ON c.serialnumber = s.serialnumber
'''
SQLQuery_node1723423704451 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"s":Steptrainerlanding_node1723421426087, "c":Customercurated_node1723421454692}, transformation_ctx = "SQLQuery_node1723423704451")

# Script generated for node Amazon S3
AmazonS3_node1723423995649 = glueContext.getSink(path="s3://chunglm-spark-datalake/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1723423995649")
AmazonS3_node1723423995649.setCatalogInfo(catalogDatabase="spark_datalake",catalogTableName="step_trainer_trusted")
AmazonS3_node1723423995649.setFormat("json")
AmazonS3_node1723423995649.writeFrame(SQLQuery_node1723423704451)
job.commit()