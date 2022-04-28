package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class Analysis5(spark: SparkSession, hiveStatement: Statement, dataframe: DataFrame){
    val sc = spark.sparkContext
    val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    val db = "ecommerce"
    val table = "alchemy"
    sqlHiveContext.sql(s"USE $db")
    hiveStatement.execute(s"USE $db")

    def paymentFailPercent(): Unit = {
        //Count Number of TXN that Pass and Fail
        dataframe.createOrReplaceTempView("FailPercent")
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding the number of failures and successes...\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        val df2 = spark.sql(s"""SELECT payment_txn_success, COUNT(payment_txn_success) AS occurrences
                                FROM FailPercent
                                GROUP BY payment_txn_success
                                ORDER BY occurrences DESC""")
        
        
        df2.show(2)
        println("############################################################")
    }
    
    def paymentFailPercentHive(): Unit = {
        //Count Number of TXN that Pass and Fail
        var query = s"""SELECT payment_txn_success, COUNT(payment_txn_success) 
                    FROM $table
                    GROUP BY payment_txn_success"""
        
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding number of transaction failures and successes...\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        var result = hiveStatement.executeQuery(query)
        while(result.next()){
            if(result.getString(1) == "N" || result.getString(1) == "Y"){
                System.out.println(result.getString(1) + "\t" + result.getString(2))
            }
        }
        println("############################################################")
    }

    def commonPaymentFail(): Unit = {
        //Count each unique failure reason and order by desc (ignores blank values due to successful txn)
        dataframe.createOrReplaceTempView("FailReason")
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding most common reason for payment failure...\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        val df2 = spark.sql(s"""SELECT failure_reason, COUNT(failure_reason) as occurrences
                          FROM FailReason
                          WHERE NOT failure_reason = ""
                          GROUP BY failure_reason
                          ORDER BY occurrences DESC LIMIT 1""")

        df2.show()
        println("############################################################")
    }

    def commonPaymentFailHive(): Unit = {
        //Count each unique failure reason and order by desc (ignores blank values due to successful txn)
        var queryList = s"""SELECT failure_reason, COUNT(failure_reason) as occurrences
                          FROM $table
                          WHERE NOT failure_reason = ""
                          GROUP BY failure_reason
                          ORDER BY occurrences DESC LIMIT 1"""		

        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding most common reason for payment failure...\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        var listReasons = hiveStatement.executeQuery(queryList)
        if(listReasons.next()){
            System.out.println(listReasons.getString(1) + "\t" + listReasons.getString(2))
        }
        println("############################################################")
    }
 }
