import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

val spark = SparkSession
   .builder()
   .appName("MasterApp")
   .enableHiveSupport()
   .getOrCreate()



val parquetdf = spark.read.parquet("/FileStore/tables/parquet/file/");
parquetdf.createOrReplaceTempView("parquetFile");

val result = spark.sql("select * from parquetFile")
result.show

result.write.partitionBy("saleEvent").parquet("/FileStore/tables/parquet-with-partition/file/")