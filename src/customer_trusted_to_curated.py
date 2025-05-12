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
CustomerTrusted_node1747080427377 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1747080427377")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1747080430881 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1747080430881")

# Script generated for node SQL Query
SqlQuery1367 = '''
SELECT DISTINCT t1.* 
FROM stedi.customer_trusted t1 
JOIN stedi.accelerometer_trusted t2 
ON t1.email = t2.user;
'''
SQLQuery_node1747073053823 = sparkSqlQuery(glueContext, query = SqlQuery1367, mapping = {"t1":CustomerTrusted_node1747080427377, "t2":AccelerometerTrusted_node1747080430881}, transformation_ctx = "SQLQuery_node1747073053823")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1747073053823, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747070392857", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1747073070816 = glueContext.getSink(path="s3://stedi-lake-house-gs/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1747073070816")
CustomerCurated_node1747073070816.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1747073070816.setFormat("json")
CustomerCurated_node1747073070816.writeFrame(SQLQuery_node1747073053823)
job.commit()