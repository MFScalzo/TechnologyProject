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
        val df2 = spark.sql(s"""SELECT payment_txn_success, COUNT(payment_txn_success) AS occurrences
                                FROM FailPercent
                                GROUP BY payment_txn_success
                                ORDER BY occurrences DESC""")

        println(s"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding the number of failures and successes...\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        df2.show(2)
        val x = df2.select("occurrences").where("payment_txn_success == F")
        val fail: Int = x.first().getInt(1)
        val y = df2.select("occurrences").where("payment_txn_success == Y")
        val pass: Int = y.first().getInt(1)
        val percent: Float = (fail / (pass + fail))*100
        println(s"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nPayments failed ${percent%.2f}% of the time")
        println("############################################################")
    }

    def paymentFailPercentHive(): Unit = {
        //Count Number of TXN that Pass and Fail
        var query = s"""SELECT payment_txn_success, COUNT(payment_txn_success) 
                    FROM $table
                    GROUP BY payment_txn_success"""
        
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding number of transaction failures and successes...\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        var result = hiveStatement.executeQuery(query)      
        var fail = 0
        var pass = 0
        println(s"Success(Y/N)\tOccurrences\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        while(result.next()){
            if(result.getString(1) == 'N'){ //|| result.getString(1) == 'Y'){
                System.out.println(s"${result.getString(1)}\t\t${result.getString(2)}")
                fail = result.getInt(2)
            }
            else if(result.getString(1) == 'Y'){
                System.out.println(f"${result.getString(1)}\t\t${result.getString(2)}")
                pass = result.getInt(2)
            }
        }
        val percent: Float = (fail /(pass + fail))*100
        println(s"~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nPayments failed ${percent%.2f}% of the time.")
        println("############################################################")

    }

    def commonPaymentFail(): Unit = {
        //Count each unique failure reason and order by desc (ignores blank values due to successful txn)
        dataframe.createOrReplaceTempView("FailReason")
        val df2 = spark.sql(s"""SELECT failure_reason, COUNT(failure_reason) as occurrences
                          FROM FailReason
                          WHERE NOT failure_reason = ""
                          GROUP BY failure_reason
                          ORDER BY occurrences DESC LIMIT 1""")

        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding most common reason for payment failure...\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
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
            System.out.println(s"Reason for Failure\tOccurrences\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n${listReasons.getString(1)}\t\t${listReasons.getString(2)}")
        }
        println("############################################################")
    }
 }
