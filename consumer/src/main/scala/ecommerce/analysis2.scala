package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class Analysis2(spark: SparkSession, hiveStatement: Statement) {
    val sc = spark.sparkContext
    val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    def highestRevenueByCountry() {
        val query = s"SELECT country, sum((qty * price)) from alchemy "
    }

    def highestRevenueByCountryHive() {
        var query = s""
        hiveStatement.execute(query)
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