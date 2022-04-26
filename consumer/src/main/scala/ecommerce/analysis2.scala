package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

class Analysis2(spark: SparkSession) {
    val sc = spark.sparkContext
    val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val tableDF = spark.read
        .option("header", true)
        .option("inferSchema", true)
        .csv("/user/maria_dev/alchemyData.csv")

    def highestRevenueByCountry() {

    }

    def highestRevenueByCountryHive() {

    }

    def mostPopularDay() {

    }

    def mostPopularDayHive() {

    }

    def mostPopularMonth() {

    }

    def mostPopularMonthHive() {

    }
}