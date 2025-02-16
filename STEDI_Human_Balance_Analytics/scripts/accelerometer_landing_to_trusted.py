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

# Script generated for node Customer_trusted
Customer_trusted_node1737921482156 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer-tz-table", transformation_ctx="Customer_trusted_node1737921482156")

# Script generated for node accelerometer_landing
accelerometer_landing_node1737921356169 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing_folder", transformation_ctx="accelerometer_landing_node1737921356169")

# Script generated for node Inner Join
InnerJoin_node1737921548258 = Join.apply(frame1=accelerometer_landing_node1737921356169, frame2=Customer_trusted_node1737921482156, keys1=["user"], keys2=["email"], transformation_ctx="InnerJoin_node1737921548258")

# Script generated for node SQL Query
SqlQuery4145 = '''
SELECT * FROM myDataSource
'''
SQLQuery_node1737922512385 = sparkSqlQuery(glueContext, query = SqlQuery4145, mapping = {"myDataSource":InnerJoin_node1737921548258}, transformation_ctx = "SQLQuery_node1737922512385")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1737922512385, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737920509984", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737923117364 = glueContext.getSink(path="s3://accelerometer--bucket/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737923117364")
AmazonS3_node1737923117364.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1737923117364.setFormat("json")
AmazonS3_node1737923117364.writeFrame(SQLQuery_node1737922512385)
job.commit()