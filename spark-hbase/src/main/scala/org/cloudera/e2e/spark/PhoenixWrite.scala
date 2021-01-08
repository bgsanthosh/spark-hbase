package org.cloudera.e2e.spark

object PhoenixWrite {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkContext
    import org.apache.spark.sql.{SQLContext, SparkSession}
    import org.apache.phoenix.spark._

    val spark = SparkSession
      .builder()
      .appName("phoenix-test")
      .master("local")
      .getOrCreate()
    try {


      // Load data from TABLE1

      val hbase_zookeeper_url = args(0)
      val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))

      spark.sparkContext.parallelize(dataSet).saveToPhoenix("OUTPUT_TEST_TABLE1", Seq("ID","COL1","COL2"),zkUrl = Some(hbase_zookeeper_url))

    } finally {
      spark.stop()


    }

  }


}
