package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class Analysis1(spark: SparkSession, hiveStatement: Statement) { 
    val sc = spark.sparkContext
    val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val databaseName = "ecommerce"
    val tableName = "alchemy"

    sqlHiveContext.sql(s"USE $databaseName")
    hiveStatement.execute(s"USE $databaseName")

    def mostPopularProductByCategory() {        // displays the most popoular product name and which category it belongs to with the quantity that was sold.
        val query = s"""
        SELECT product_category, product_name, SUM(qty) as quantitySold
        FROM $tableName
        GROUP BY product_category, product_name
        ORDER BY quantitySold DESC LIMIT 1;
        """"
        sqlHiveContext.sql(query).show()
    }
    
    def mostPopularProductByCategoryHive() {    // Using Hive: displays the most popular product and which category it belongs to with the quantity that was sold.
        val query = s"""
        SELECT product_category, product_name, SUM(qty) as quantitySold
        FROM $tableName
        GROUP BY product_category, product_name
        ORDER BY quantitySold DESC LIMIT 1;
        """"

        val result = hiveStatement.executeQuery(query)
        if (result.next()) {
            System.out.println(result.getString(1) + "\t" + result.getString(2));
        }
    }

    def highestRevenueProductByCategory(){      // displays the product and its category that has the highest revenue and that revenue.
        val query = s"SELECT product_category, product_name, SUM(qty * price) as revenue FROM $tableName GROUP BY product_category, product name ORDER BY revenue DESC LIMIT 1;"
        sqlHiveContext.sql(query).show()
    }
        
    def highestRevenueProductByCategoryHive() { // Using Hive: displays the product and its category that has the highest revenue and that revenue.
        val query = s"SELECT product_category, product_name, SUM(qty * price) as revenue FROM $tableName GROUP BY product_category, product name ORDER BY revenue DESC LIMIT 1;"
        val result = hiveStatement.executeQuery(query)
        
        if (result.next()) {
            System.out.println(result.getString(1) + "\t" + result.getString(2));
        } 
        
    def highestRevenueProductByMonth() {        //NOT WORKING: I believe it is because the datetime column is named as such, it is not recognized as a valid column here.......
                                                // displays the highest revenue product and the month in which it sold.
        val query = s"SELECT DATEPART(MM, datetime) as month, product_name, SUM(qty * price) as revenue FROM $tableName GROUP BY month, product_name ORDER BY revenue DESC LIMIT 1;"
        sqlHiveContext.sql(query).show()
    }

    def highestRevenueProductByMonthHive() {    // NOT WORKING: see above.             Using Hive: displays the highest revenue product and the month in which it sold.
        val query =s"SELECT DATEPART(MM, datetime) as month, product_name, SUM(qty * price) as revenue FROM $tableName GROUP BY month, product_name ORDER BY revenue DESC LIMIT 1;"
        val result = hiveStatement.executeQuery(query)
        if (result.next()) {
            System.out.println(result.getString(1) + "\t" + result.getString(2));
        }
    }
}