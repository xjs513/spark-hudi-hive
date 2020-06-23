package kasa.tech.mor

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.config.HoodieWriteConfig.TABLE_NAME
import org.apache.hudi.index.HoodieIndex
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteSyncHiveMor {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val start: Long = System.currentTimeMillis()
    val spark: SparkSession = SparkSession.builder()
      .appName("WriteSyncHiveMor")
      .master("local[*]")
      // hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val dataFrame: DataFrame = spark.read.format("json").load("/kasa_test/record.json")

    //dataFrame.show()

    val tableName = "record_mor"
    // 保存到了本地路径, 改为 HDFS 路径
    val basePath = "/hudi_test/record_mor"

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

      // 设置jdbc 连接同步
      //.option(HIVE_URL_OPT_KEY, "jdbc:hive2://kasa:10000")
      // 设置要同步的hive库名
      .option(HIVE_DATABASE_OPT_KEY, "kasa_warehouse")
      // 设置要同步的hive表名
      .option(HIVE_TABLE_OPT_KEY, tableName)
      // 设置要同步的分区列名
      .option(HIVE_PARTITION_FIELDS_OPT_KEY, "city")
      // 设置数据集注册并同步到hive
      .option(HIVE_SYNC_ENABLED_OPT_KEY, "true")
      // 设置当分区变更时，当前数据的分区目录是否变更
      .option(HoodieIndexConfig.BLOOM_INDEX_UPDATE_PARTITION_PATH, "true")
      // 用于将分区字段值提取到Hive分区列中的类,这里我选择使用当前分区的值同步
      .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, "org.apache.hudi.hive.MultiPartKeysValueExtractor")
      // 设置索引类型目前有HBASE,INMEMORY,BLOOM,GLOBAL_BLOOM 四种索引 为了保证分区变更后能找到必须设置全局GLOBAL_BLOOM
      .option(HoodieIndexConfig.INDEX_TYPE_PROP, HoodieIndex.IndexType.GLOBAL_BLOOM.name())

      .option(HIVE_USE_JDBC_OPT_KEY, "false")
      // 并行度参数设置
      .option("hoodie.insert.shuffle.parallelism", "2")
      .option("hoodie.upsert.shuffle.parallelism", "2")
      .mode(Append)
      .save(basePath);

    /**/
    // 读取数据 这个可以运行
    // spark.read.format("org.apache.hudi").load(basePath + "/*").show

    val end: Long = System.currentTimeMillis()

    println("Time used: [" + (end - start) + "] milliseconds.")

    spark.stop()
  }
}
