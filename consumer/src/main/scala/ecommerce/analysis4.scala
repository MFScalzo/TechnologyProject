package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class Analysis4(spark: SparkSession, hiveStatement: Statement) {  
    val sc = spark.sparkContext
    val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val databaseName = "project2"
    val tableName = "alchemy"

    sqlHiveContext.sql(s"USE $databaseName")
    hiveStatement.execute(s"USE $databaseName")

    def mostPopularPaymentType() {
        val query = s"""
        SELECT payment_type,  COUNT(payment_type) as occurrence
        FROM $tableName
        GROUP BY payment_type
        ORDER BY occurrence DESC LIMIT 1;
        """"
        sqlHiveContext.sql(query).show()
    }
    
    def mostPopularPaymentTypeHive() {
        val query = s"""
        SELECT payment_type, COUNT(payment_type) as occurrence
        FROM $tableName
        GROUP BY payment_type
        ORDER BY occurrence DESC LIMIT 1;
        """"

        val result = hiveStatement.executeQuery(query)
        if (result.next()) {
            System.out.println(result.getString(1) + "\t" + result.getString(2));
        }
    }
    

   
    

    def PaymentTypeWithHighestRevenue() {
        val query = s"""
        SELECT payment_type, SUM(qty * price) as revenue
        FROM $tableName
        GROUP BY payment_type
        ORDER BY revenue DESC LIMIT 1;""""

        sqlHiveContext.sql(query).show()
    }
        
    def PaymentTypeWithHighestRevenueHive() {
        val query = s"""
        SELECT payment_type, SUM(qty * price) as revenue
        FROM $tableName
        GROUP BY payment_type
        ORDER BY revenue DESC LIMIT 1;""""

        val result = hiveStatement.executeQuery(query)
        
        if (result.next()) {
            System.out.println(result.getString(1) + "\t" + result.getString(2));
        } 
    }
}