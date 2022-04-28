package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class Analysis4(spark: SparkSession, hiveStatement: Statement) {
    def mostPopularPaymentTypeHive() {println("mostPopularPaymentTypeHive()")}
    def PaymentTypeWithHighestRevenueHive() {println("PaymentTypeWithHighestRevenueHive()")}
    def mostPopularPaymentType() {println("mostPopularPaymentType()")}
    def PaymentTypeWithHighestRevenue() {println("PaymentTypeWithHighestRevenue()")}
}