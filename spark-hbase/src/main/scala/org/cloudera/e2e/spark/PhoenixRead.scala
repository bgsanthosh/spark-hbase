package org.cloudera.e2e.spark

import java.sql.Date

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.sql.SparkSession
import org.apache.phoenix.spark._

object PhoenixRead {


  def main(args: Array[String]): Unit = {

    import org.apache.spark.SparkContext
    import org.apache.spark.sql.{SQLContext, SparkSession}

    val spark = SparkSession
      .builder()
      .appName("phoenix-test")
      .master("local")
      .getOrCreate()
    try {


    // Load data from TABLE1
    val hbase_zookeeper_url = args(0)
    val df = spark.sqlContext
      .read
      .format("org.apache.phoenix.spark")
      .options(Map("table" -> "OUTPUT_TEST_TABLE1", "zkUrl" -> hbase_zookeeper_url))
      .load

      df.show()
      //val dataSet = List((1L, "1", 1), (2L, "2", 2), (3L, "3", 3))

      //spark.sparkContext.parallelize(dataSet).saveToPhoenix("OUTPUT_TEST_TABLE", Seq("ID","COL1","COL2"),zkUrl = Some(hbase_zookeeper_url))

    } finally {
      spark.stop()


    }

  }

}
