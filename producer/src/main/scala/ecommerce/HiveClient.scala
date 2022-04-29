package ecommerce

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class HiveClient {
    val dataBaseName = "ecommerce"
    val tableName = "vanquish"

    def loadIntoHDFS(local: String, hdfs: String) {
        val conf = new Configuration()
        val fs = FileSystem.get(conf)

        val localpath = new Path(local)
        val hdfspath = new Path(hdfs)

        // Check if the file exists, if so delete it
        if(fs.exists(hdfspath)) {
            fs.delete(hdfspath)
        }

        fs.copyFromLocalFile(localpath, hdfspath)
    }

    def loadIntoHive(hdfsPath: String): Unit = {
        var connection: java.sql.Connection = null

        try {
            var driverName = "org.apache.hive.jdbc.HiveDriver"
            val connectionString = s"jdbc:hive2://sandbox-hdp.hortonworks.com:10000/$dataBaseName"

            Class.forName(driverName)

            connection = DriverManager.getConnection(connectionString, "", "")
            val statement = connection.createStatement()

            // println("Dropping Database if it exists...")
            // var query = s"DROP DATABASE IF EXISTS $dataBaseName CASCADE"
            // statement.execute(query)

            println("Creating the Database...")
            var query = s"CREATE DATABASE IF NOT EXISTS $dataBaseName"
            statement.execute(query)

            println("Dropping old vanquish Table...")
            query = s"DROP TABLE IF EXISTS $tableName"
            statement.execute(query)
            
            println("Creating new table...")
            // If we want to use datetime as an actual timestamp format we have to add a seoncds position at the end of it, for now it is type string and must be converted later
            query = s"""CREATE EXTERNAL TABLE $tableName (order_id int, customer_id int, customer_name string, product_id int, product_name string, product_category string, payment_type string, qty int, price float, datetime string, country string, city string, ecommerce_website_name string, payment_txn_id int, payment_txn_success char(1), failure_reason string)
                row format delimited
                fields terminated by ','
                LOCATION '$hdfsPath'
                """
            statement.execute(query)
        }
        catch {
            case ex => {
                ex.printStackTrace()
                throw new Exception(s"${ex.getMessage}")
            }
        }
        finally {
            try {
                if (connection != null) {
                    connection.close()
                }
            } 
            catch {
                case ex => {
                    ex.printStackTrace()
                    throw new Exception(s"${ex.getMessage}")
                }
            }
        }
    }
}


