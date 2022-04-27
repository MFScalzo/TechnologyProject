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

    val databaseName = "project2"
    val tableName = "alchemy"

    sqlHiveContext.sql(s"USE $databaseName")
    hiveStatement.execute(s"USE $databaseName")

    def mostPopularProductByCategory() {
        val query = s"""
        SELECT product_category, product_name, SUM(qty) as quantitySold
        FROM $tableName
        GROUP BY product_category, product_name
        ORDER BY quantitySold DESC LIMIT 1;
        """"
        sqlHiveContext.sql(query).show()
    }
    
    def mostPopularProductByCategoryHive() {
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
    

   
    }

    def highestRevenueProductByCategory(){
        val query = s"SELECT product_category, SUM(qty * price) as revenue FROM $tableName GROUP BY product_category ORDER BY revenue DESC LIMIT 1;"
        sqlHiveContext.sql(query).show()
    }
        
    def highestRevenueProductByCategoryHive() {
        val query = s"SELECT product_category, SUM(qty * price) as revenue FROM $tableName GROUP BY product_category ORDER BY revenue DESC LIMIT 1;"
        val result = hiveStatement.executeQuery(query)
        
        if (result.next()) {
            System.out.println(result.getString(1) + "\t" + result.getString(2));
        } 
        
    def highestRevenueProductByMonth() {
        val query = s"SELECT datetime(1), SUM(qty * price) as revenue FROM $tableName GROUP BY datetime(1) ORDER BY revenue DESC LIMIT 1;"
        sqlHiveContext.sql(query).show()
    }

    def highestRevenueProductByMonthHive() {
        val query = s"SELECT datetime(1), SUM(qty * price) as revenue FROM $tableName GROUP BY datetime(1) ORDER BY revenue DESC LIMIT 1;"
        val result = hiveStatement.executeQuery(query)
        if (result.next()) {
            System.out.println(result.getString(1) + "\t" + result.getString(2));
        }
    }
}