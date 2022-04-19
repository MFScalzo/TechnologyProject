package ecommerce

import java.io.IOException
import scala.util.Try
import java.sql.Connection
import java.sql.Statement
import java.sql.DriverManager

class HiveClient {
    val dataBaseName = "Project2"
    def loadCSV(filePath: String): Unit = {
        var connection: java.sql.Connection = null

        try {
            var driverName = "org.apache.hive.jdbc.HiveDriver"
            val connectionString = "jdbc:hive2://sandbox-hdp.hortonworks.com:10000/default"

            connection = DriverManager.getConnection(connectionString, "", "")
            val statement = connection.createStatement()

            var query = s"CREATE DATABASE [IF NOT EXISTS] $dataBaseName;"

        }
        catch {
            
        }
    }
}


