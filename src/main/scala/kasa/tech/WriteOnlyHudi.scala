package kasa.tech

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteOnlyHudi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val spark: SparkSession = SparkSession.builder()
      .appName("WriteOnlyHudi")
      .master("local[*]")
      // hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val dataFrame: DataFrame = spark.read.format("json").load("/kasa_test/record.json")

    //dataFrame.show()

    val tableName = "record"
    // 保存到了本地路径, 改为 HDFS 路径
    val basePath = "/hudi_test/record"

    val start: Long = System.currentTimeMillis()

    dataFrame.write.format("org.apache.hudi")
      // 配置读时合并
      .option(TABLE_TYPE_OPT_KEY, MOR_TABLE_TYPE_OPT_VAL)
      // 设置主键列名
      .option(RECORDKEY_FIELD_OPT_KEY, "id")
      // 设置数据更新时间的列名
      .option(PRECOMBINE_FIELD_OPT_KEY, "ts")
      // 分区列设置
      .option(PARTITIONPATH_FIELD_OPT_KEY, "city")
      // hudi表名称设置
      .option(TABLE_NAME, tableName)
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(Append)
      .save(basePath)

    val end: Long = System.currentTimeMillis()

    println("Time used: [" + (end - start) + "] milliseconds.")

    spark.stop()

  }
}
