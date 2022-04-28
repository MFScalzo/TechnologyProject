package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class Analysis5(spark: SparkSession, hiveStatement: Statement) {
    def paymentFailPercentHive() {println("paymentFailPercentHive()")}
    def commonPaymentFailHive() {println("commonPaymentFailHive()")}
    def paymentFailPercent() {println("paymentFailPercent()")}
    def commonPaymentFail() {println("commonPaymentFail()")}
}