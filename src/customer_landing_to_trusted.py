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

# Script generated for node CustomerLandingNode
CustomerLandingNode_node1746966792491 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-gs/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLandingNode_node1746966792491")

# Script generated for node SQL Query
SqlQuery1017 = '''
select * from customer_landing where shareWithResearchAsOfDate!=0
'''
SQLQuery_node1746968566659 = sparkSqlQuery(glueContext, query = SqlQuery1017, mapping = {"customer_landing":CustomerLandingNode_node1746966792491}, transformation_ctx = "SQLQuery_node1746968566659")

# Script generated for node CustomerTrustedZone
EvaluateDataQuality().process_rows(frame=SQLQuery_node1746968566659, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746966787328", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerTrustedZone_node1746966797344 = glueContext.getSink(path="s3://stedi-lake-house-gs/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrustedZone_node1746966797344")
CustomerTrustedZone_node1746966797344.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrustedZone_node1746966797344.setFormat("json")
CustomerTrustedZone_node1746966797344.writeFrame(SQLQuery_node1746968566659)
job.commit()