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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1737917583712 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing_folder", transformation_ctx="AWSGlueDataCatalog_node1737917583712")

# Script generated for node remove nulls from sharewithresearchasofdate
SqlQuery4004 = '''
select * from myDataSource
where sharewithresearchasofdate is not null;
'''
removenullsfromsharewithresearchasofdate_node1737917618745 = sparkSqlQuery(glueContext, query = SqlQuery4004, mapping = {"myDataSource":AWSGlueDataCatalog_node1737917583712}, transformation_ctx = "removenullsfromsharewithresearchasofdate_node1737917618745")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=removenullsfromsharewithresearchasofdate_node1737917618745, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737917561664", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1737917698337 = glueContext.getSink(path="s3://customer--bucket/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1737917698337")
AmazonS3_node1737917698337.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer-tz-table")
AmazonS3_node1737917698337.setFormat("glueparquet", compression="snappy")
AmazonS3_node1737917698337.writeFrame(removenullsfromsharewithresearchasofdate_node1737917618745)
job.commit()