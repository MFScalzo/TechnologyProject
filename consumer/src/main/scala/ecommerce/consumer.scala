package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager


object consumer {
    def main(args: Array[String]) {
        // Establish Hive Connection and initialize statement object
        var connection: java.sql.Connection = null
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val connectionString = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/ecommerce"
        Class.forName(driverName)
        connection = DriverManager.getConnection(connectionString, "", "")
        // This is the object you can call .execute(query) on
        val hiveStatement = connection.createStatement()

        // Initialize SparkSession object
        val spark = SparkSession
            .builder()
            .appName("Consumer")
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        
        // initialize analysis objects
        val drakeFunctions = new Analysis1(spark, hiveStatement)
        val mattFunctions = new Analysis2(spark, hiveStatement)
        val javierFunctions = new Analysis3(spark, hiveStatement)
        val davidFunctions = new Analysis4(spark, hiveStatement)
        val nickFunctinos = new Analysis5(spark, hiveStatement)
    }
}