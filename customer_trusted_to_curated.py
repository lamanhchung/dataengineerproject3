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

# Script generated for node Accelerometer landing
Accelerometerlanding_node1723363972386 = glueContext.create_dynamic_frame.from_catalog(database="spark_datalake", table_name="accelerometer_landing", transformation_ctx="Accelerometerlanding_node1723363972386")

# Script generated for node Customer trusted
Customertrusted_node1723363938639 = glueContext.create_dynamic_frame.from_catalog(database="spark_datalake", table_name="customer_trusted", transformation_ctx="Customertrusted_node1723363938639")

# Script generated for node SQL Query
SqlQuery0 = '''
select distinct(user) as `user` from myDataSource
'''
SQLQuery_node1723395545019 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Accelerometerlanding_node1723363972386}, transformation_ctx = "SQLQuery_node1723395545019")

# Script generated for node Join
Join_node1723364009171 = Join.apply(frame1=Customertrusted_node1723363938639, frame2=SQLQuery_node1723395545019, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1723364009171")

# Script generated for node Drop Fields
DropFields_node1723364076242 = DropFields.apply(frame=Join_node1723364009171, paths=["user"], transformation_ctx="DropFields_node1723364076242")

# Script generated for node Customer curated
Customercurated_node1723364855085 = glueContext.getSink(path="s3://chunglm-spark-datalake/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customercurated_node1723364855085")
Customercurated_node1723364855085.setCatalogInfo(catalogDatabase="spark_datalake",catalogTableName="customer_curated")
Customercurated_node1723364855085.setFormat("json")
Customercurated_node1723364855085.writeFrame(DropFields_node1723364076242)
job.commit()