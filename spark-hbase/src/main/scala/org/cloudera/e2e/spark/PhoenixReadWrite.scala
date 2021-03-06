package org.cloudera.e2e.spark

object PhoenixReadWrite {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkContext
    import org.apache.spark.sql.{SQLContext, SparkSession}
    import org.apache.phoenix.spark._

    val spark = SparkSession
      .builder()
      .appName("phoenix-test")
      .getOrCreate()
    try {


      // Load data from TABLE1

      val hbase_zookeeper_url = args(0)
      val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))

      spark.sparkContext.parallelize(dataSet).saveToPhoenix("OUTPUT_TEST_TABLE", Seq("ID","COL1","COL2"),zkUrl = Some(hbase_zookeeper_url))


      val df = spark.sqlContext
        .read
        .format("org.apache.phoenix.spark")
        .options(Map("table" -> "OUTPUT_TEST_TABLE", "zkUrl" -> hbase_zookeeper_url))
        .load()

      df.show()
      println("row count is:" + df.count())


    } finally {
      spark.stop()


    }

  }


}
