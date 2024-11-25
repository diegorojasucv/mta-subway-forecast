import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
import gs_repartition
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

# Script generated for node Amazon S3
AmazonS3_node1731873030719 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://mta-subway/raw-data/MTA_Subway_Hourly_Ridership__Beginning_July_2020.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1731873030719")

# Script generated for node SQL Query
SqlQuery12913 = '''
with stage as (

    select
    
        left(transit_timestamp, 10) as created_date,
        station_complex_id,
        station_complex,
        max(latitude) as latitude,
        max(longitude) as longitude,
        max(georeference) as georeference,
        sum(ridership) as number_of_riderships
    
    from raw_data
    where
        transit_mode = 'subway'
        and borough = 'Manhattan'
        --and station_complex_id in (611, 610, 607, 602, 628, 164, 614, 318, 616, 447)
    group by 1,2,3
    
)
select 

    station_complex_id,
    station_complex,
    latitude,
    longitude,
    georeference,
    number_of_riderships,
    created_date,
    right(created_date, 4) as year_period
    
from stage
where right(created_date, 4) >= 2023
'''
SQLQuery_node1731878752429 = sparkSqlQuery(glueContext, query = SqlQuery12913, mapping = {"raw_data":AmazonS3_node1731873030719}, transformation_ctx = "SQLQuery_node1731878752429")

# Script generated for node Evaluate Data Quality (Multiframe)
EvaluateDataQualityMultiframe_node1731881883517_ruleset = """
    Rules = [
        IsComplete "station_complex_id",
        IsComplete "created_date",
        ColumnCount = 8,
        ColumnValues "number_of_riderships" > 0
    ]
"""

EvaluateDataQualityMultiframe_node1731881883517 = EvaluateDataQuality().process_rows(frame=SQLQuery_node1731878752429, ruleset=EvaluateDataQualityMultiframe_node1731881883517_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQualityMultiframe_node1731881883517", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node Autobalance Processing
AutobalanceProcessing_node1731884475190 = SQLQuery_node1731878752429.gs_repartition(numPartitionsStr="1")

# Script generated for node Amazon S3
AmazonS3_node1731873241063 = glueContext.getSink(path="s3://mta-subway/data-transformed/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1731873241063")
AmazonS3_node1731873241063.setCatalogInfo(catalogDatabase="default",catalogTableName="mta_subway_top_stations")
AmazonS3_node1731873241063.setFormat("csv")
AmazonS3_node1731873241063.writeFrame(AutobalanceProcessing_node1731884475190)
job.commit()