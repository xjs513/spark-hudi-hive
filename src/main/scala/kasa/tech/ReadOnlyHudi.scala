package kasa.tech

import org.apache.spark.sql.SparkSession

object ReadOnlyHudi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val spark: SparkSession = SparkSession.builder()
      .appName("WriteOnlyHudi")
      .master("local[*]")
      // hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // val tableName = "record"
    // 保存到了本地路径, 改为 HDFS 路径
    val basePath = "/hudi_test/record"

    // 读取数据 这个可以运行
    spark.read.format("org.apache.hudi").load(basePath + "/*").show


  }
}
