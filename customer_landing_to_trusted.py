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

# Script generated for node Customer landing
Customerlanding_node1723348190762 = glueContext.create_dynamic_frame.from_catalog(database="spark_datalake", table_name="customer_landing", transformation_ctx="Customerlanding_node1723348190762")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from myDataSource where sharewithresearchasofdate is not null
'''
SQLQuery_node1723348223871 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Customerlanding_node1723348190762}, transformation_ctx = "SQLQuery_node1723348223871")

# Script generated for node Customer trusted
Customertrusted_node1723348233157 = glueContext.getSink(path="s3://chunglm-spark-datalake/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Customertrusted_node1723348233157")
Customertrusted_node1723348233157.setCatalogInfo(catalogDatabase="spark_datalake",catalogTableName="customer_trusted")
Customertrusted_node1723348233157.setFormat("json")
Customertrusted_node1723348233157.writeFrame(SQLQuery_node1723348223871)
job.commit()