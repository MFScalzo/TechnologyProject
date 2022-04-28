package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class Analysis2(spark: SparkSession, hiveStatement: Statement, dataFrame: DataFrame) {
    val sc = spark.sparkContext
    val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val databaseName = "ecommerce"
    val tableName = "alchemy"

    sqlHiveContext.sql(s"USE $databaseName")
    hiveStatement.execute(s"USE $databaseName")

    def highestRevenueByCountry() {
        println("Ordering Countries by Highest Revenue...")
    }

    def highestRevenueByCountryHive() {
        val query = s"""SELECT country, SUM(qty * price) as revenue
                        FROM $tableName
                        GROUP BY country
                        ORDER BY revenue DESC"""

        val result = hiveStatement.executeQuery(query)

        if (result.next()) {    // probably have to do some sort of formatting to make it look like $
            System.out.println(f"${result.getString(1)}\t$$${result.getString(2).toFloat}%.2f");
        }
    }

    def mostPopularDay() {
        println("mostPopularDay()")
    }

    def mostPopularDayHive() {
        println("mostPopularDayHive()")
    }

    def mostPopularMonth() {
        println("mostPopularMonth()")
    }

    def mostPopularMonthHive() {
        println("mostPopularMonthHive()")
    }
}