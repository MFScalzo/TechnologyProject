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
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        println("Analyze with Hive or Spark?")
        println("1. Hive")
        println("2. Spark")
        println("0. Exit")
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

        var option = -1

        try {
            option = readLine.toInt
        }
        catch {
            case _: Throwable => option = -1
        }

        return option
    }

    def functionMenu(): Int = {
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        println("Which analysis you would like to run?")
        println("1.  Most Popular Product")
        println("2.  Highest Revenue Product")
        println("3.  Countries by Desending Total Revenue")
        println("4.  Customer Who Spent the Least")
        println("5.  Customer Who Spent the Most")
        println("6.  Average Amount Spent")
        println("7.  Customer Who Ordered the Most Products")
        println("8.  Most Popular Payment Type")
        println("9.  Highest Revenue Payment Type")
        println("10. Transaction Failures and Successes")
        println("11. Most Common Reason for Payment Failure")
        println("0.  Back to Menu")
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")

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
        
        var continueLoop = true

        while (continueLoop) {

            val optionSelected = functionMenu()

            if (optionSelected == 0) {
                println("Going Back...")
                continueLoop = false
            }

            if (optionSelected == 1) {
                drakeFunctions.mostPopularProductByCategoryHive()
                print("Continue...")
                readLine
            }

            if (optionSelected == 2) {
                drakeFunctions.highestRevenueProductHive()
                print("Continue...")
                readLine
            }

            if (optionSelected == 3) {
                mattFunctions.highestRevenueByCountryHive()
                print("Continue...")
                readLine
            }
            
            if (optionSelected == 4) {
                javierFunctions.transactionMinAmountHive()
                print("Continue...")
                readLine
            }
            
            if (optionSelected == 5) {
                javierFunctions.transactionMaxAmountHive()
                print("Continue...")
                readLine
            }

            if (optionSelected == 6) {
                javierFunctions.transactionAvgAmountHive()
                print("Continue...")
                readLine
            }

            if (optionSelected == 7) {
                javierFunctions.mostProductsPerOrderHive()
                print("Continue...")
                readLine
            }
            
            if (optionSelected == 8) {
                davidFunctions.mostPopularPaymentTypeHive()
                print("Continue...")
                readLine
            }

            if (optionSelected == 9) {
                davidFunctions.paymentTypeWithHighestRevenueHive()
                print("Continue...")
                readLine
            }

            if (optionSelected == 10) {
                nickFunctions.paymentFailPercentHive()
                print("Continue...")
                readLine
            }

            if (optionSelected == 11) {
                nickFunctions.commonPaymentFailHive()
                print("Continue...")
                readLine
            }
        }
    }

    def runSparkFunctions() {
        
        var continueLoop = true

        while (continueLoop) {

            val optionSelected = functionMenu()

            if (optionSelected == 0) {
                println("Going Back...")
                continueLoop = false
            }

            if (optionSelected == 1) {
                drakeFunctions.mostPopularProductByCategory()
                print("Continue...")
                readLine
            }

            if (optionSelected == 2) {
                drakeFunctions.highestRevenueProduct()
                print("Continue...")
                readLine
            }

            if (optionSelected == 3) {
                mattFunctions.highestRevenueByCountry()
                print("Continue...")
                readLine
            }
            
            if (optionSelected == 4) {
                javierFunctions.transactionMinAmount()
                print("Continue...")
                readLine
            }
            
            if (optionSelected == 5) {
                javierFunctions.transactionMaxAmount()
                print("Continue...")
                readLine
            }

            if (optionSelected == 6) {
                javierFunctions.transactionAvgAmount()
                print("Continue...")
                readLine
            }

            if (optionSelected == 7) {
                javierFunctions.mostProductsPerOrder()
                print("Continue...")
                readLine
            }
            
            if (optionSelected == 8) {
                davidFunctions.mostPopularPaymentType()
                print("Continue...")
                readLine
            }

            if (optionSelected == 9) {
                davidFunctions.paymentTypeWithHighestRevenue()
                print("Continue...")
                readLine
            }

            if (optionSelected == 10) {
                nickFunctions.paymentFailPercent()
                print("Continue...")
                readLine
            }

            if (optionSelected == 11) {
                nickFunctions.commonPaymentFail()
                print("Continue...")
                readLine
            }
        }
    }
}