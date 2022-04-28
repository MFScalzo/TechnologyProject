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

    val df1 = dataFrame

    def mostPopularProductByCategory() {        // displays the most popoular product name and which category it belongs to with the quantity that was sold.
        println("Doing spark stuff here")
        df1.createOrReplaceTempView("popularProduct")
        val df2 = spark.sql(s"SELECT product_category, product_name, SUM(qty) as quantitySold FROM popularProduct GROUP BY product_category, product_name ORDER BY quantitySold DESC LIMIT 1")
        df2.show()
    }
    
    def mostPopularProductByCategoryHive() {    // Using Hive: displays the most popular product and which category it belongs to with the quantity that was sold.
        val query = s"""
        SELECT product_category, product_name, SUM(qty) as quantitySold
        FROM $tableName
        GROUP BY product_category, product_name
        ORDER BY quantitySold DESC LIMIT 1
        """
        
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding the most popular Product\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        val result = hiveStatement.executeQuery(query)
        if (result.next()) {
            System.out.println(f"Product Category\tProduct Name\tQuantity Sold\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n${result.getString(1)}\t\t${result.getString(2)}\t\t${result.getString(3)}");
        }
    }

    def highestRevenueProduct(){      // displays the product and its category that has the highest revenue and that revenue.
        println("doing Spark Stuff here")
        df1.createOrReplaceTempView("revenueProduct")
        val df2 = spark.sql(s"SELECT product_category, product_name, SUM(qty * price) as revenue FROM revenueProduct GROUP BY product_category, product_name ORDER BY revenue DESC LIMIT 1")
        df2.show()
    }
        
    def highestRevenueProductHive() { // Using Hive: displays the product and its category that has the highest revenue and that revenue.
        val query = s"SELECT product_category, product_name, $$SUM(qty * price)%.2f as revenue FROM $tableName GROUP BY product_category, product_name ORDER BY revenue DESC LIMIT 1"
        val result = hiveStatement.executeQuery(query)
        
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding the product with the highest revenue\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        if (result.next()) {
            System.out.println(f"Product Category\tProduct Name\tProduct Revenue\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n${result.getString(1)}\t\t${result.getString(2)}\t\t$$${result.getString(3).toFloat}%.2f");
        }
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    }
}