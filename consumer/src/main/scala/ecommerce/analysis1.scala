package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class Analysis1(spark: SparkSession, hiveStatement: Statement) {

    def mostPopularProductByCategoryHive() { println("mostPopularProductByCategoryHive()")}
    def highestRevenueProductByCategoryHive() { println("highestRevenueProductByCategoryHive()")}
    def highestRevenueProductByMonthHive() { println("highestRevenueProductByMonthHive()")}
    def mostPopularProductByCategory() { println("mostPopularProductByCategory()")}
    def highestRevenueProductByCategory() { println("highestRevenueProductByCategory()")}
    def highestRevenueProductByMonth() { println("highestRevenueProductByMonth()")}
}