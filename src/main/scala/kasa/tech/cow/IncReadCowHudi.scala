package kasa.tech.cow

import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 增量查询
  */
object IncReadCowHudi {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val spark: SparkSession = SparkSession.builder()
      .appName("IncReadCowHudi")
      .master("local[*]")
      // hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    // val tableName = "record"
    // 保存到了本地路径, 改为 HDFS 路径
    val basePath = "/hudi_test/record"

    // 读取数据 这个可以运行
    val roViewDF = spark.read.format("org.apache.hudi").load(basePath + "/*")

    roViewDF.createOrReplaceTempView("hudi_ro_table")

    // 获取 commit 时间戳
    val commits: DataFrame = spark.sql("select distinct(_hoodie_commit_time) as commitTime from hudi_ro_table order by commitTime")

    import spark.implicits._

    val beginTime: String = commits.map(row => row.getString(0)).take(1)(0)

      //.take(1).apply(1).getAs(String)

    // 设置起始时间戳为上次时间戳
    System.out.println("beginTime = [" + beginTime + "]")

    // 读取数据 这个可以运行
    val incViewDF = spark
      .read
      // QUERY_TYPE_SNAPSHOT_OPT_VAL  可以
      // QUERY_TYPE_READ_OPTIMIZED_OPT_VAL
      // QUERY_TYPE_INCREMENTAL_OPT_VAL
      // 不能增量读取
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20200622170000")
      .format("org.apache.hudi").load(basePath + "/*/*")

    incViewDF.show(false)

    spark.stop()
  }
}
