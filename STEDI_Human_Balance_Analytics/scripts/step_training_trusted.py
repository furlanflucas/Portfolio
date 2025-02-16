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

# Script generated for node landing_step
landing_step_node1738177106133 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing_folder", transformation_ctx="landing_step_node1738177106133")

# Script generated for node customer_curated
customer_curated_node1738177140104 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1738177140104")

# Script generated for node SQL Query
SqlQuery5570 = '''
SELECT s.* 
FROM step_landing s
WHERE s.serialnumber IN (
    SELECT DISTINCT c.serialnumber
    FROM customer_curated c
);
'''
SQLQuery_node1738177238519 = sparkSqlQuery(glueContext, query = SqlQuery5570, mapping = {"step_landing":landing_step_node1738177106133, "customer_curated":customer_curated_node1738177140104}, transformation_ctx = "SQLQuery_node1738177238519")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1738177238519, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738175503996", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1738177519305 = glueContext.getSink(path="s3://step-trainer--bucket/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1738177519305")
AmazonS3_node1738177519305.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_training_trusted")
AmazonS3_node1738177519305.setFormat("glueparquet", compression="snappy")
AmazonS3_node1738177519305.writeFrame(SQLQuery_node1738177238519)
job.commit()