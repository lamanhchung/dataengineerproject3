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

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1723424729751 = glueContext.create_dynamic_frame.from_catalog(database="spark_datalake", table_name="accelerometer_trusted", transformation_ctx="Accelerometertrusted_node1723424729751")

# Script generated for node Step trainer trusted
Steptrainertrusted_node1723424754578 = glueContext.create_dynamic_frame.from_catalog(database="spark_datalake", table_name="step_trainer_trusted", transformation_ctx="Steptrainertrusted_node1723424754578")

# Script generated for node SQL Query
SqlQuery0 = '''
select a.timestamp, a.x, a.y, a.z, s.serialnumber, s.distancefromobject from a inner join s on a.timestamp = s.sensorreadingtime
'''
SQLQuery_node1723424782269 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":Accelerometertrusted_node1723424729751, "s":Steptrainertrusted_node1723424754578}, transformation_ctx = "SQLQuery_node1723424782269")

# Script generated for node Meachine learning curated
Meachinelearningcurated_node1723424812491 = glueContext.getSink(path="s3://chunglm-spark-datalake/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Meachinelearningcurated_node1723424812491")
Meachinelearningcurated_node1723424812491.setCatalogInfo(catalogDatabase="spark_datalake",catalogTableName="machine_learning_curated")
Meachinelearningcurated_node1723424812491.setFormat("json")
Meachinelearningcurated_node1723424812491.writeFrame(SQLQuery_node1723424782269)
job.commit()