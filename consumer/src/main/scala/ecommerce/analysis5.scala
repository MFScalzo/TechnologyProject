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
    
    val db = "project2"
    val table = "alchemy"

    sqlHiveContext.sql(s"USE $db")

    def paymentFailPercent(): Unit = {
        //Count Number of TXN that Pass
        var queryP = s"""SELECT COUNT(payment_txn_sucess) as passCount
                    FROM $table
                    WHERE payment_txn_sucess = 'Y';"""

        //Count Number of TXN that Fail
        var queryF = s"""SELECT COUNT(payment_txn_sucess) as failCount
                    FROM $table
                    WHERE payment_txn_sucess = 'N';"""

        var passAmount: Int = sqlHiveContext.sql(queryP).first.getInt(0)            //Get Pass as Int type
        var failAmount: Int = sqlHiveContext.sql(queryF).first.getInt(0)            //Get Fail as Int type
        var failPercent: Float = (failAmount / (passAmount + failAmount))*100       //Calculate the Fail Percent as Float type
        
        println(s"Transactions fail ${failPercent}% of the time.")
    }

    def commonPaymentFail(): Unit = {
        var commonFailure = " "
        var queryList = s"SELECT DISTINCT failure_reason FROM $table;"		//Get Unique Failure Reasons

        var listReasons = sqlHiveContext.sql(queryList).rdd.map(r=>r.toString()).collect()
        var count = 0
        for(reason <- listReasons){
            var query = s"""SELECT COUNT(failure_reason)
                        FROM $table
                        WHERE failure_reason = '$reason'"""
            var temp = sqlHiveContext.sql(query).first.getInt(0)
            if(temp > count){
                count = temp
                commonFailure = reason
            }
        }
        println(s"The most common reason for transaction error is: $commonFailure")
    }

 }