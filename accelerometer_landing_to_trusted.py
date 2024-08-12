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

# Script generated for node Customer trusted
Customertrusted_node1723352709236 = glueContext.create_dynamic_frame.from_catalog(database="spark_datalake", table_name="customer_trusted", transformation_ctx="Customertrusted_node1723352709236")

# Script generated for node Accelerometer landing
Accelerometerlanding_node1723351820364 = glueContext.create_dynamic_frame.from_catalog(database="spark_datalake", table_name="accelerometer_landing", transformation_ctx="Accelerometerlanding_node1723351820364")

# Script generated for node Share with researcher
Sharewithresearcher_node1723352639410 = Join.apply(frame1=Accelerometerlanding_node1723351820364, frame2=Customertrusted_node1723352709236, keys1=["user"], keys2=["email"], transformation_ctx="Sharewithresearcher_node1723352639410")

# Script generated for node Drop Fields
DropFields_node1723352996629 = DropFields.apply(frame=Sharewithresearcher_node1723352639410, paths=["email", "phone"], transformation_ctx="DropFields_node1723352996629")

# Script generated for node Accelerometer trusted
Accelerometertrusted_node1723352785056 = glueContext.getSink(path="s3://chunglm-spark-datalake/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Accelerometertrusted_node1723352785056")
Accelerometertrusted_node1723352785056.setCatalogInfo(catalogDatabase="spark_datalake",catalogTableName="accelerometer_trusted")
Accelerometertrusted_node1723352785056.setFormat("json")
Accelerometertrusted_node1723352785056.writeFrame(DropFields_node1723352996629)
job.commit()