package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class Analysis5(spark: SparkSession, hiveStatement: Statement){
    val sc = spark.sparkContext
    val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val path = "home/users/maria_dev/ecommerce/alchemy/producer.csv"
    val data = spark.read.csv(path)

    def paymentFailPercent(): Unit = {
        var txnPass = data.groupBy("payment_txn_success").agg(
            .count(when(col("payment_txn_success") == 'Y', True)))
        var txnFail = data.groupBy("payment_txn_success").agg(
            .count(when(col("payment_txn_success") == 'N', True)))
        var failPercent = (txnFail/(txnPass + txnFail))*100

        println("Transactions fail ", failPercent, "% of the time.")

    }

    def commonPaymentFail(): Unit = {
        var commonFailure = " "
        var listFailReasons = data.select("failure_reason").distinct().map(f=>f.getString(0)).collect.toList
        var count = 0
        for (reason <- listFailReasons){
            var temp = test.groupBy("failure_reason").agg(count(when(col("failure_reason") == i, True)))
            if(temp > count){
                count = temp
                commonFailure = i
            }

        }
        println("The most common reason for transaction error is: ", commonFailure)
    }

 }