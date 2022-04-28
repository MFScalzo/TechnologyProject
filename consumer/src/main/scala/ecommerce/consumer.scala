package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

import scala.io.StdIn.readLine


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
    
    // Initialize analysis objects
    val drakeFunctions = new Analysis1(spark, hiveStatement)
    val mattFunctions = new Analysis2(spark, hiveStatement)
    val javierFunctions = new Analysis3(spark, hiveStatement)
    val davidFunctions = new Analysis4(spark, hiveStatement)
    val nickFunctions = new Analysis5(spark, hiveStatement)

    def main(args: Array[String]) {
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
        drakeFunctions.highestRevenueProductByCategoryHive()

        mattFunctions.highestRevenueByCountryHive()

        //javierFunctions.

        davidFunctions.mostPopularPaymentTypeHive()
        davidFunctions.PaymentTypeWithHighestRevenueHive()  // Functions should not be capitalized

        nickFunctions.paymentFailPercentHive()
        nickFunctions.commonPaymentFailHive()
    }

    def runSparkFunctions() {
        drakeFunctions.mostPopularProductByCategory()
        drakeFunctions.highestRevenueProductByCategory()

        mattFunctions.highestRevenueByCountry()

        //javierFunctions.

        davidFunctions.mostPopularPaymentType()
        davidFunctions.PaymentTypeWithHighestRevenue()  // Functions should not be

        nickFunctions.paymentFailPercent()
        nickFunctions.commonPaymentFail()
    }
}