# Glue-hudi-cdc
Sample cdc code for Glue spark streaming

Using Glue studio with dependencies:
s3://dpg-chen-test/jars/hudi-spark-bundle_2.11-0.9.0.jar,s3://dpg-chen-test/jars/spark-avro_2.11-2.4.4.jar,s3://dpg-chen-test/jars/spark-streaming-kinesis-asl_2.11-2.4.4.jar

Also need to add job parameter:
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer
