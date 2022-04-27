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

    val databaseName = "project2"
    val tableName = "alchemy"

    spark.sql(s"USE $databaseName")
    hiveStatement.execute(s"USE $databaseName")

    def highestRevenueByCountry() {
        val query = s"""SELECT country, SUM(qty * price) as revenue
                        FROM $tableName
                        GROUP BY country
                        ORDER BY revenue DESC LIMIT 1;""""

        spark.sql(query).show()
    }

    def highestRevenueByCountryHive() {
        var query = s"""SELECT country, SUM(qty * price) as revenue
                        FROM $tableName
                        GROUP BY country
                        ORDER BY revenue DESC LIMIT 1;""""

        val result = hiveStatement.executeQuery(query)

        if (result.next()) {
            System.out.println(result.getString(1) + "\t" + result.getString(2));
        }
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