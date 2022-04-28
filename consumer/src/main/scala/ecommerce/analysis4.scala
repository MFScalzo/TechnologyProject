package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class Analysis4(spark: SparkSession, hiveStatement: Statement, dataFrame: DataFrame) {  
    val sc = spark.sparkContext
    val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val df1 = dataFrame

    val databaseName = "ecommerce"
    val tableName = "alchemy"

    sqlHiveContext.sql(s"USE $databaseName")
    hiveStatement.execute(s"USE $databaseName")

    def mostPopularPaymentType() {
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding most Popular Payment Type...\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        df1.createOrReplaceTempView("popularPaymentType")
        val df2 = spark.sql(s"SELECT payment_type, COUNT(payment_type) as occurrence FROM popularPaymentType GROUP BY payment_type ORDER BY  occurrence DESC LIMIT 1")
        df2.show()
        println("############################################################")
    }
    
    def mostPopularPaymentTypeHive() {
        val query = s"""
        SELECT payment_type, COUNT(payment_type) as occurrence
        FROM $tableName
        GROUP BY payment_type
        ORDER BY occurrence DESC LIMIT 1
        """
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding the most Popular Payment Type...\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        val result = hiveStatement.executeQuery(query)
        println("Payment Type\t\tOccurences")
        if (result.next()) {
            System.out.println(result.getString(1) + "\t\t" + result.getString(2));
        }
        println("############################################################")
    }

    def paymentTypeWithHighestRevenue() {
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding Payment Type with the Highest Revenue...\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        df1.createOrReplaceTempView("paymentRevenue")
        val df2 = spark.sql(s"SELECT payment_type, SUM(qty * price) as revenue FROM paymentRevenue GROUP BY payment_type ORDER BY revenue DESC LIMIT 1")
        df2.show()
        println("############################################################")
    }
        
    def paymentTypeWithHighestRevenueHive() {
        val query = s"""
        SELECT payment_type, SUM(qty * price) as revenue
        FROM $tableName
        GROUP BY payment_type
        ORDER BY revenue DESC LIMIT 1"""
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding Payment Type with the Highest Revenue...\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        val result = hiveStatement.executeQuery(query)
        
        println("Payment Type\t\tRevenue")
        if (result.next()) {
            System.out.println(f"${result.getString(1)}\t$$${result.getString(2).toFloat}%.2f");
        }
        println("############################################################")
    }
}
