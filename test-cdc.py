import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import DataFrame, Row
import datetime
from awsglue import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

tableName = 'dpg'
hudi_options = {
  'hoodie.table.name': tableName,
  'hoodie.datasource.write.recordkey.field': 'id',
  'hoodie.datasource.write.table.name': tableName,
  'hoodie.datasource.write.operation': 'upsert',
  'hoodie.datasource.write.precombine.field': 'created_at',
  'hoodie.upsert.shuffle.parallelism': 2,
  'hoodie.insert.shuffle.parallelism': 2,
  'hoodie.datasource.write.partitionpath.field': 'partitionpath',
  'hoodie.datasource.hive_sync.enable': 'true',
  'hoodie.datasource.hive_sync.database': 'chen',
  'hoodie.datasource.hive_sync.table': tableName,
  'hoodie.datasource.hive_sync.use_jdbc': 'false',
  'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
  'hoodie.datasource.hive_sync.partition_fields': 'dt',
}

hudi_delete_options = {
  'hoodie.table.name': tableName,
  'hoodie.datasource.write.recordkey.field': 'id',
  'hoodie.datasource.write.partitionpath.field': 'partitionpath',
  'hoodie.datasource.write.table.name': tableName,
  'hoodie.datasource.write.operation': 'delete',
  'hoodie.datasource.write.precombine.field': 'created_at',
  'hoodie.upsert.shuffle.parallelism': 2, 
  'hoodie.insert.shuffle.parallelism': 2
}

# Script generated for node S3 bucket
dataframe_S3bucket_node1 = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": "arn:aws:kinesis:ap-northeast-1:094083608210:stream/testcdc2",
        "classification": "json",
        "startingPosition": "latest",
        "inferSchema": "true",
    },
    transformation_ctx="dataframe_S3bucket_node1",
)

def processBatch(data_frame, batchId):
    if data_frame.count() > 0:
        S3bucket_node1 = DynamicFrame.fromDF(data_frame, glueContext, "from_data_frame")
        # Script generated for node ApplyMapping
        ApplyMapping_node1636349093718 = ApplyMapping.apply(
            frame=S3bucket_node1,
            mappings=[
                ("id", "string", "id", "short"),
                ("name", "string", "name", "string"),
                ("age", "string", "age", "string"),
                ("created_at", "string", "created_at", "string"),
                ("type", "string", "type", "string"),
            ],
            transformation_ctx="ApplyMapping_node1636349093718",
        )        
        basePath = 's3://dpg-chen-test/testcdcdpg/'

        df = ApplyMapping_node1636349093718.toDF()
        
        print('this is test')
        df.show(100,0)
        df.filter(col('type') != 'delete').write.format("hudi"). \
          options(**hudi_options). \
          mode("append"). \
          save(basePath)
        df.filter(col('type') == 'delete').write.format("hudi"). \
          options(**hudi_delete_options). \
          mode("append"). \
          save(basePath)


glueContext.forEachBatch(
    frame=dataframe_S3bucket_node1,
    batch_function=processBatch,
    options={
        "windowSize": "100 seconds",
        "checkpointLocation": args["TempDir"] + "/" + 'testcdcdpg' + "/checkpoint/",
    },
)
job.commit()
