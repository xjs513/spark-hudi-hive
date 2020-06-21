package kasa.tech

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadHiveTable {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Spark-test-1")
      .master("local[*]")
      // hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()

//    val dataFrame: DataFrame = spark.sql("select * from hudi_db.record_ro where `_hoodie_partition_path`='BJ' and id=8")

    val dataFrame: DataFrame = spark.sql("select * from hudi_db.record_rt where city='BJ' and id=8")

    dataFrame.show(false)

    spark.stop()
  }
}
