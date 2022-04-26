package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object consumer {
    def main(args: Array[String]) {
        val spark = SparkSession
            .builder()
            .appName("Consumer")
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
    }
}