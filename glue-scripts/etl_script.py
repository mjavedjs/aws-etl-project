import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Amazon S3
AmazonS3_node1772784441346 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://stockprice-mj/input/"], "recurse": True}, transformation_ctx="AmazonS3_node1772784441346")

# Script generated for node Drop Fields
DropFields_node1772784533736 = DropFields.apply(frame=AmazonS3_node1772784441346, paths=["date", "symbol"], transformation_ctx="DropFields_node1772784533736")

# Script generated for node Amazon S3 sink
EvaluateDataQuality().process_rows(frame=DropFields_node1772784533736, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772784355254", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
additional_options = {"path": "s3://stockprice-mj/collacted/", "write.parquet.compression-codec": "snappy"}
AmazonS3sink_node1772784816211_df = DropFields_node1772784533736.toDF()
AmazonS3sink_node1772784816211_df.write.format("delta").options(**additional_options).mode("append").save()

job.commit()