package kasa.tech.mor

import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.SparkSession

object ReadMorHudi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val spark: SparkSession = SparkSession.builder()
      .appName("ReadMorHudi")
      .master("local[*]")
      // hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // val tableName = "record"
    // 保存到了本地路径, 改为 HDFS 路径
    val basePath = "/hudi_test/record_mor"

    // 读取数据 这个可以运行
    val roViewDF = spark
      .read
      .format("org.apache.hudi")
      // QUERY_TYPE_SNAPSHOT_OPT_VAL  可以  默认
      // QUERY_TYPE_READ_OPTIMIZED_OPT_VAL  Copy on Write 模式不支持这种读取格式
      // QUERY_TYPE_INCREMENTAL_OPT_VAL  HoodieException: Incremental view not implemented yet, for merge-on-read tables
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .load(basePath)

    roViewDF.createOrReplaceTempView("hudi_mor_table")
    spark.sql("select `_hoodie_commit_time`, id,name from hudi_mor_table where city='SH' AND `_hoodie_commit_time`<20200622302111").show()

    spark.stop()

    // 只能读取到最近一次提交的数据
  }
}
