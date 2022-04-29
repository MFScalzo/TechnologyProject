package ecommerce

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class Analysis3(spark: SparkSession, hiveStatement: Statement, dataFrame: DataFrame) { 
    val sc = spark.sparkContext
    val sqlHiveContext = new org.apache.spark.sql.hive.HiveContext(sc)

    val databaseName = "ecommerce"
    val tableName = "alchemy"

    sqlHiveContext.sql(s"USE $databaseName")
    hiveStatement.execute(s"USE $databaseName")

    val df1 = dataFrame

    // Average, max, min transaction amount 
    // customer_id, customer_name, order_id, (price * qty) as total

    // min
    def transactionMinAmount(): Unit = {
        df1.createOrReplaceTempView("MinAmount")
        val df2 = spark.sql(s"SELECT customer_id, customer_name, COUNT(order_id) as `total_orders`, SUM(price * qty) as `total_price` FROM MinAmount GROUP BY customer_id, customer_name ORDER BY total_price")
        df2.show()
    }

    def transactionMinAmountHive(): Unit = {
         val query = s"""SELECT customer_id, customer_name, COUNT(order_id) as `total_orders`, SUM(price * qty) as `total_price`
                         FROM $tableName
                         GROUP BY customer_id, customer_name
                         ORDER BY total_price
                    """
        val result = hiveStatement.executeQuery(query)   
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding the minimum transaction amount\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        if (result.next()) {
            System.out.println(f"${result.getString(1)}\t${result.getString(2)}\t${result.getString(3)}\t${"$" + result.getString(4).toString}")
        }
    }

    // max
    def transactionMaxAmount(): Unit = {
        df1.createOrReplaceTempView("MaxAmount")
        val df3 = spark.sql(s"SELECT customer_id, customer_name, COUNT(order_id) as `total_orders`, SUM(price * qty) as `total_price` FROM MaxAmount GROUP BY customer_id, customer_name ORDER BY total_price DESC")
        df3.show()
    }

    def transactionMaxAmountHive(): Unit = {
        val query = s"""SELECT customer_id, customer_name, COUNT(order_id) as `total_orders`, SUM(price * qty) as `total_price`
                        FROM $tableName
                        GROUP BY customer_id, customer_name
                        ORDER BY total_price DESC
                    """
        val result = hiveStatement.executeQuery(query)
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding the maximum transaction amount\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        if (result.next()) {
            System.out.println(f"${result.getString(1)}\t${result.getString(2)}\t${result.getString(3)}\t${"$" + result.getString(4).toString}")
        }
    }

    // avg
    def transactionAvgAmount(): Unit =  {
        df1.createOrReplaceTempView("AvgAmount")
        val df4 = spark.sql(s"SELECT COUNT(order_id) as `total_orders`, ROUND(AVG(x.totalPrice)) as `avg_txn_amt` FROM (SELECT order_id, COUNT(order_id) as totalOrders, customer_id,  SUM(price * qty) as totalPrice FROM avgAmount GROUP BY customer_id, order_id ORDER BY totalPrice DESC) as x ORDER BY avg_txn_amt")
        df4.show()
    }

    def transactionAvgAmountHive(): Unit = {
        val query = s"""SELECT COUNT(order_id) as `total_orders`, ROUND(AVG(x.totalPrice)) as `avg_txn_amt`
                        FROM 
                        (
                            SELECT order_id, COUNT(order_id) as totalOrders, customer_id, SUM(price * qty) as totalPrice
                            FROM $tableName
                            GROUP BY customer_id, order_id
                            ORDER BY totalPrice DESC
                        ) as x
                        ORDER BY avg_txn_amt
                        """
            val result = hiveStatement.executeQuery(query)
            println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\nFinding the average transaction amount\n~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
            if (result.next()) {
                System.out.println(f"${result.getString(1)}\t${"$" + result.getString(2).toString}")
            }
    }

    // Customer with most products in their order / money spent
    // customer_id, customer_name (optional?), order_id, qty

    def mostProductsPerOrder(): Unit = {
        df1.createOrReplaceTempView("MostProducts")
        val df5 = spark.sql(s"SELECT customer_id, customer_name, COUNT(order_id) as `total_orders`, SUM(qty) AS `total_amount` FROM MostProducts GROUP BY customer_id, customer_name ORDER BY total_amount DESC")
        df5.show()
    }

    def mostProductsPerOrderHive(): Unit = {
        val query = s"""SELECT customer_id, customer_name, COUNT(order_id) as `total_orders`, SUM(qty) AS `total_amount`
                        FROM $tableName
                        GROUP BY customer_id, customer_name
                        ORDER BY total_amount DESC
                    """
        val result = hiveStatement.executeQuery(query)
        println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Finding the most Products per Order~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        if(result.next()) {
            System.out.println(f"${result.getString(1)}\t${result.getString(2)}\t${result.getString(3)}\t${result.getString(4)}")
        }
    }
}
