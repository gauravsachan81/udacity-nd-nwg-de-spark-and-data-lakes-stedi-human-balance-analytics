import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1746969925550 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1746969925550")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1746969921357 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1746969921357")

# Script generated for node SQL Query
SqlQuery1252 = '''
SELECT
t1.*,
t2.*
FROM
stedi.customer_trusted t1
INNER JOIN
stedi.accelerometer_landing t2
ON
t1.email = t2.user;
'''
SQLQuery_node1747077771151 = sparkSqlQuery(glueContext, query = SqlQuery1252, mapping = {"t1":CustomerTrusted_node1746969925550, "t2":AccelerometerLanding_node1746969921357}, transformation_ctx = "SQLQuery_node1747077771151")

# Script generated for node SQL Query
SqlQuery1251 = '''
select user, timestamp, x, y , z from myDataSource
'''
SQLQuery_node1746974324101 = sparkSqlQuery(glueContext, query = SqlQuery1251, mapping = {"myDataSource":SQLQuery_node1747077771151}, transformation_ctx = "SQLQuery_node1746974324101")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1746974324101, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746966787328", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1746969987966 = glueContext.getSink(path="s3://stedi-lake-house-gs/acceleromter/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1746969987966")
AccelerometerTrusted_node1746969987966.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1746969987966.setFormat("json")
AccelerometerTrusted_node1746969987966.writeFrame(SQLQuery_node1746974324101)
job.commit()