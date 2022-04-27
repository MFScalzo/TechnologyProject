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
    hiveStatement.execute(s"USE $db")

    def paymentFailPercent(): Unit = {
        //Count Number of TXN that Pass
        var queryP = s"""SELECT COUNT(payment_txn_success) as passCount
                    FROM $table
                    WHERE payment_txn_success = 'Y';"""

        //Count Number of TXN that Fail
        var queryF = s"""SELECT COUNT(payment_txn_success) as failCount
                    FROM $table
                    WHERE payment_txn_success = 'N';"""

        var pass = sqlHiveContext.sql(queryP).show()
        var fail = sqlHiveContext.sql(queryF).show()
        var passAmount: Int = sqlHiveContext.sql(queryP).first.getInt(0)            //Get Pass as Int type
        var failAmount: Int = sqlHiveContext.sql(queryF).first.getInt(0)            //Get Fail as Int type
        var failPercent: Float = (failAmount / (passAmount + failAmount))*100       //Calculate the Fail Percent as Float type
        
        println(s"Transactions fail ${failPercent}% of the time.")
    }
    
    // def paymentFailPercentHive(): Unit = {
    //     //Count Number of TXN that Pass
    //     var queryP = s"""SELECT COUNT(payment_txn_success) as passCount
    //                 FROM $table
    //                 WHERE payment_txn_success = 'Y';"""

    //     //Count Number of TXN that Fail
    //     var queryF = s"""SELECT COUNT(payment_txn_success) as failCount
    //                 FROM $table
    //                 WHERE payment_txn_success = 'N';"""

    //     var pass = sqlHiveContext.sql(queryP)
    //     if(pass.next()){
    //         System.out.println(pass.getString(1))
    //     }
    //     var fail = sqlHiveContext.sql(queryF)
    //     if(fail.next()){
    //         System.out.println(fail.getString(1))
    //     }
    //     var passAmount: Int = sqlHiveContext.sql(queryP).first.getInt(0)            //Get Pass as Int type
    //     var failAmount: Int = sqlHiveContext.sql(queryF).first.getInt(0)            //Get Fail as Int type
    //     var failPercent: Float = (failAmount / (passAmount + failAmount))*100       //Calculate the Fail Percent as Float type
        
    //     println(s"Transactions fail ${failPercent}% of the time.")
    // }

    def commonPaymentFail(): Unit = {
        var commonFailure = " "
        var queryList = s"""SELECT failure_reason, COUNT(failure_reason)
                          FROM $table
                          GROUP BY failure_reason
                          ORDER BY COUNT(failure_reason) DESC;"""		//Get Unique Failure Reasons

        var listReasons = sqlHiveContext.sql(queryList).show()
        
        //println(s"The most common reason for transaction error is: $commonFailure")
    }

    // def commonPaymentFailHive(): Unit = {
    //     var commonFailure = " "
    //     var queryList = s"""SELECT failure_reason, COUNT(failure_reason)
    //                       FROM $table
    //                       GROUP BY failure_reason
    //                       ORDER BY COUNT(failure_reason) DESC;"""		//Get Unique Failure Reasons

    //     var listReasons = sqlHiveContext.sql(queryList)
    //     if(listReasons.next()){
    //         System.out.println(listReasons.getString(1))
    //     }
        
    //     //println(s"The most common reason for transaction error is: $commonFailure")
    // }

 }