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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1747081281841 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1747081281841")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1747081286137 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1747081286137")

# Script generated for node SQL Query
SqlQuery1341 = '''
SELECT 
t1.*,
t2.timestamp
FROM 
stedi.step_trainer_trusted t1 
INNER JOIN 
stedi.accelerometer_trusted t2 
ON 
t1.sensorreadingtime = t2.timestamp
'''
SQLQuery_node1747074041704 = sparkSqlQuery(glueContext, query = SqlQuery1341, mapping = {"t2":AccelerometerTrusted_node1747081281841, "t1":StepTrainerTrusted_node1747081286137}, transformation_ctx = "SQLQuery_node1747074041704")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1747074041704, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747072896056", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1747074044488 = glueContext.getSink(path="s3://stedi-lake-house-gs/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1747074044488")
AmazonS3_node1747074044488.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1747074044488.setFormat("json")
AmazonS3_node1747074044488.writeFrame(SQLQuery_node1747074041704)
job.commit()