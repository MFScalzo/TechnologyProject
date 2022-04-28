package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager


object consumer {
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

    val schema = new StructType()
        .add("order_id", IntegerType, true)
        .add("customer_id", IntegerType, true)
        .add("customer_name", StringType, true)
        .add("prod_id", IntegerType, true)
        .add("product_name", StringType, true)
        .add("product_category", StringType, true)
        .add("payment_type", StringType, true)
        .add("qty", IntegerType, true)
        .add("price", FloatType, true)
        .add("datetime", StringType, true)
        .add("country", StringType, true)
        .add("city", StringType, true)
        .add("ecommerce_website_name", StringType, true)
        .add("payment_txn_id", IntegerType, true)
        .add("payment_txn_success", StringType, true)
        .add("failure_reason", StringType, true)

    // Initialize Spark DataFrame
    println("Creating DataFrame...")
    val dataFrame = spark.read
        .format("csv")
        .schema(schema)
        .load("/user/maria_dev/alchemy/ecommerce.csv")
    
    // Initialize analysis objects
    val drakeFunctions = new Analysis1(spark, hiveStatement, dataFrame)
    val mattFunctions = new Analysis2(spark, hiveStatement, dataFrame)
    val javierFunctions = new Analysis3(spark, hiveStatement, dataFrame)
    val davidFunctions = new Analysis4(spark, hiveStatement, dataFrame)
    val nickFunctions = new Analysis5(spark, hiveStatement, dataFrame)

    def main(args: Array[String]) {
        // Establish Hive Connection and initialize statement object
        var connection: java.sql.Connection = null
        var driverName = "org.apache.hive.jdbc.HiveDriver"
        val connectionString = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/ecommerce"
        Class.forName(driverName)
        connection = DriverManager.getConnection(connectionString, "", "")
        // This is the object you can call .execute(query) on
        val hiveStatement = connection.createStatement()

        var continueLoop = true

        while (continueLoop) {

            val optionSelected = menu()

            if (optionSelected == 0) {
                println("Exiting...")
                continueLoop = false
            }

            if (optionSelected == 1) {
                println("You selected Hive.")
                runHiveFunctions()
            }

            if (optionSelected == 2) {
                println("You selected Spark.")
                runSparkFunctions()
            }
        }
    }

    def menu(): Int = {
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        println("Analize with Hive or Spark?")
        println("1. Hive")
        println("2. Spark")
        println("0. Exit")
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        var option = -1

        try {
            option = readLine.toInt
        }
        catch {
            case _: Throwable => option = -1
        }

        return option
    }

    def runHiveFunctions() {
        drakeFunctions.mostPopularProductByCategoryHive()
        print("Continue...")
        readLine
        drakeFunctions.highestRevenueProductHive()
        print("Continue...")
        readLine
        mattFunctions.highestRevenueByCountryHive()
        print("Continue...")
        readLine
        //javierFunctions.
        
        davidFunctions.mostPopularPaymentTypeHive()
        print("Continue...")
        readLine
        davidFunctions.paymentTypeWithHighestRevenueHive()
        print("Continue...")
        readLine

        nickFunctions.paymentFailPercentHive()
        print("Continue...")
        readLine
        nickFunctions.commonPaymentFailHive()
        print("Continue...")
        readLine
    }

    def runSparkFunctions() {
        drakeFunctions.mostPopularProductByCategory()
        print("Continue...")
        readLine
        drakeFunctions.highestRevenueProduct()
        print("Continue...")
        readLine

        mattFunctions.highestRevenueByCountry()
        print("Continue...")
        readLine

        //javierFunctions.

        davidFunctions.mostPopularPaymentType()
        print("Continue...")
        readLine
        davidFunctions.paymentTypeWithHighestRevenue()
        print("Continue...")
        readLine

        nickFunctions.paymentFailPercent()
        print("Continue...")
        readLine
        nickFunctions.commonPaymentFail()
        print("Continue...")
        readLine
    }
}