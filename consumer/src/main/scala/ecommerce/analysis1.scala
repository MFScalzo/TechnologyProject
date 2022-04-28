package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager


class Analysis1(spark: SparkSession, hiveStatement: Statement, dataFrame: DataFrame) { 
    val sc = spark.sparkContext
    val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val databaseName = "ecommerce"
    val tableName = "alchemy"

    sqlHiveContext.sql(s"USE $databaseName")
    hiveStatement.execute(s"USE $databaseName")

    def mostPopularProductByCategory() {        // displays the most popoular product name and which category it belongs to with the quantity that was sold.
        println("Doing spark stuff here")
    }
    
    def mostPopularProductByCategoryHive() {    // Using Hive: displays the most popular product and which category it belongs to with the quantity that was sold.
        val query = s"""
        SELECT product_category, product_name, SUM(qty) as quantitySold
        FROM $tableName
        GROUP BY product_category, product_name
        ORDER BY quantitySold DESC LIMIT 1
        """
        
        println("Finding most popular Product by Category...")
        val result = hiveStatement.executeQuery(query)
        if (result.next()) {
            System.out.println(f"${result.getString(1)}\t${result.getString(2)}\t${result.getString(3)}");
        }
    }

    def highestRevenueProduct(){      // displays the product and its category that has the highest revenue and that revenue.
        println("doing Spark Stuff here")
    }
        
    def highestRevenueProductHive() { // Using Hive: displays the product and its category that has the highest revenue and that revenue.
        val query = s"SELECT product_category, product_name, SUM(qty * price) as revenue FROM $tableName GROUP BY product_category, product_name ORDER BY revenue DESC LIMIT 1"
        val result = hiveStatement.executeQuery(query)
        
        println("Finding the highest revenue Product...")
        if (result.next()) {
            System.out.println(f"${result.getString(1)}\t${result.getString(2)}\t${result.getString(3).toFloat}%.2f");
        } 
    }
}