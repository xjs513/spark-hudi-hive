package kasa.tech.cow

import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.SparkSession

object ReadCowHudi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val spark: SparkSession = SparkSession.builder()
      .appName("ReadCowHudi")
      .master("local[*]")
      // hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // val tableName = "record"
    // 保存到了本地路径, 改为 HDFS 路径
    val basePath = "/hudi_test/record"

    // 读取数据 这个可以运行
    val roViewDF = spark.read
      .format("org.apache.hudi")
      // QUERY_TYPE_SNAPSHOT_OPT_VAL  可以
      // QUERY_TYPE_READ_OPTIMIZED_OPT_VAL  Copy on Write 模式不支持这种读取格式
      // QUERY_TYPE_INCREMENTAL_OPT_VAL
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, 20200622170000L)
      .load(basePath) // For incremental query, pass in the root/base path of table 增量读取时只传基本路径

      // .load(basePath + "/*/*")

    roViewDF.createOrReplaceTempView("hudi_ro_table")
    spark.sql("select `_hoodie_commit_time`, id,name from hudi_ro_table where city='SH' AND `_hoodie_commit_time`>=20200622171807").show()

    spark.stop()

    // 只能读取到最近一次提交的数据
  }
}
