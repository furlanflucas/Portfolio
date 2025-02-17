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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1738179427815 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1738179427815")

# Script generated for node step_training_trusted
step_training_trusted_node1738179443246 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_training_trusted", transformation_ctx="step_training_trusted_node1738179443246")

# Script generated for node SQL Query
SqlQuery5431 = '''
SELECT 
    a.user AS customer_id,
    a.timestamp AS accelerometer_timestamp,
    a.x AS accelerometer_x,
    a.y AS accelerometer_y,
    a.z AS accelerometer_z,
    s.sensorreadingtime AS step_trainer_timestamp,
    s.distancefromobject AS step_trainer_distance
FROM accelerometer_trusted a
JOIN step_training_trusted s
ON a.timestamp = s.sensorreadingtime;
'''
SQLQuery_node1738179455512 = sparkSqlQuery(glueContext, query = SqlQuery5431, mapping = {"accelerometer_trusted":accelerometer_trusted_node1738179427815, "step_training_trusted":step_training_trusted_node1738179443246}, transformation_ctx = "SQLQuery_node1738179455512")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1738179455512, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738178125655", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1738179731984 = glueContext.getSink(path="s3://step-trainer--bucket/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1738179731984")
machine_learning_curated_node1738179731984.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1738179731984.setFormat("glueparquet", compression="snappy")
machine_learning_curated_node1738179731984.writeFrame(SQLQuery_node1738179455512)
job.commit()