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
    
    val db = "Ecommerce"
    val table = "alchemy"

    sqlHiveContext.sql(s"USE $db")
    hiveStatement.execute(s"USE $db")

    def paymentFailPercent(): Unit = {
        //Count Number of TXN that Pass and Fail
        var query = s"""SELECT payment_txn_success, COUNT(payment_txn_success) 
                    FROM $table
                    GROUP BY payment_txn_success;"""

        sqlHiveContext.sql(query).show()
    }
    
    def paymentFailPercentHive(): Unit = {
        //Count Number of TXN that Pass and Fail
        var query = s"""SELECT payment_txn_success, COUNT(payment_txn_success) 
                    FROM $table
                    GROUP BY payment_txn_success;"""
        
        var result = hiveStatement.executeQuery(query)
        if(result.next()){
            System.out.println(result.getString(1) + "\t" + result.getString(2))
        }
    
    }

    def commonPaymentFail(): Unit = {
        //Count each unique failure reason and order by desc (ignores blank values due to successful txn)
        var queryList = s"""SELECT failure_reason, COUNT(failure_reason) AS occurrences
                          FROM $table
                          WHERE NOT failure_reason = ""
                          GROUP BY failure_reason
                          ORDER BY occurrences DESC;"""		

        sqlHiveContext.sql(queryList).show()
        
    }

    def commonPaymentFailHive(): Unit = {
        //Count each unique failure reason and order by desc (ignores blank values due to successful txn)
        var queryList = s"""SELECT failure_reason, COUNT(failure_reason) as occurrences
                          FROM $table
                          WHERE NOT failure_reason = ""
                          GROUP BY failure_reason
                          ORDER BY occurrences DESC;"""		

        var listReasons = hiveStatement.executeQuery(queryList)
        if(listReasons.next()){
            System.out.println(listReasons.getString(1))
        }
    }

 }