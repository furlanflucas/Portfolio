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
Customer_trusted_node1737921482156 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="Customer_trusted_node1737921482156")

# Script generated for node accelerometer_landing
accelerometer_landing_node1737921356169 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_landing_node1737921356169")

# Script generated for node Inner Join
InnerJoin_node1737921548258 = Join.apply(frame1=accelerometer_landing_node1737921356169, frame2=Customer_trusted_node1737921482156, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="InnerJoin_node1737921548258")

# Script generated for node SQL Query
SqlQuery4802 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource
'''
SQLQuery_node1737922512385 = sparkSqlQuery(glueContext, query = SqlQuery4802, mapping = {"myDataSource":InnerJoin_node1737921548258}, transformation_ctx = "SQLQuery_node1737922512385")

# Script generated for node Customer-curated2
EvaluateDataQuality().process_rows(frame=SQLQuery_node1737922512385, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1737920509984", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Customercurated2_node1737923117364 = glueContext.getSink(path="s3://customer--bucket/customer_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="Customercurated2_node1737923117364")
Customercurated2_node1737923117364.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
Customercurated2_node1737923117364.setFormat("json")
Customercurated2_node1737923117364.writeFrame(SQLQuery_node1737922512385)
job.commit()