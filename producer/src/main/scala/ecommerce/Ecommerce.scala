package ecommerce

import scala.util.Random
import ecommerce.{Order, Customer}
import java.io._
import scala.collection.mutable.ArrayBuffer

object Ecommerce {
    val orderGenerator = new Order()
    val customerGenerator = new Customer()
    var currentTransactionID = 1
    val failureReason = List("Invalid Card Number", "Payment Declined", "Transaction Error", "Insufficient Funds")
    val maxCustomers = 10
    val maxOrdersPerCustomer = 3
    
    def main(args: Array[String]) {
        val rad = new scala.util.Random
        val numberOfCustomers = rad.nextInt(maxCustomers)
        var rowsOfData = ArrayBuffer[String]()

        println("Generating Data...")
        for(i <- 0 to numberOfCustomers) {
            val numberOfOrders = rad.nextInt(maxOrdersPerCustomer)
            val randomCustomer = customerGenerator.generateCustomerInfo()

            for(i <- 0 to numberOfOrders) {
                val randomOrder = orderGenerator.generateOrderInfo()
                val timestamp = generateDateTime()
                // Random chance of success
                val success = {if (rad.nextFloat() > 0.75f) {'N'} else {'Y'}}
                val reason = {if(success == 'N') {generateRandomReason()} else {""}}

                // Loops for every product in the product vector from the order Tuple
                for(product <- randomOrder._1) {
                    val currentOrder = Tuple8(product._1, product._2, product._3, product._4, product._5, randomOrder._2, randomOrder._3, randomOrder._4)
                    rowsOfData += generateRow(randomCustomer, currentOrder, timestamp, success, reason)
                }

                incTransactionID()
            }
        }

        println("Writing Data to CSV...")
        appendRowsToCSV(rowsOfData)

        println("Uploading CSV to HDFS...")
    }

    def appendRowsToCSV(rows: ArrayBuffer[String]): Unit = {
        val file = new File("./vanquishData.csv")
        val bw = new BufferedWriter(new FileWriter(file))

        for(row <- rows) {
            bw.write(row+"\n")
        }
        bw.close()
    }

    def incTransactionID(): Unit = {             
        currentTransactionID += 1
    }

    def generateDateTime(): String = {
        val r =  new scala.util.Random
        val firstYear = 2000
        val lastYear = 2021
        val randomYear = firstYear + r.nextInt( (lastYear - firstYear))

        val firstMonth = 1
        val lastMonth = 12
        val randomMonth = firstMonth + r.nextInt( (lastMonth - firstMonth))

        val firstDay = 1
        val lastDay = 30
        val randomDay = firstDay + r.nextInt( (lastDay - firstDay))

        val firstHour = 0
        val lastHour = 23
        val randomHour = firstHour + r.nextInt( (lastHour - firstHour))

        val firstMinute = 0
        val lastMinute = 59
        val randomMinute = firstMinute + r.nextInt( (lastMinute - firstMinute))

        return f"$randomYear-$randomMonth-$randomDay $randomHour%02d:$randomMinute%02d"
    }

    def generateRandomReason(): String = {
        return failureReason(Random.nextInt(failureReason.length))
    }

    def generateRow(customerInfo: Tuple4[String, String, String, Int], orderInfo: Tuple8[String, String, Float, Int, Int, String, String, Int], timestamp: String, success: Char, reason: String): String = { //Tuple16[Int, Int, String, Int, String, String, String, Int, Float, String, String, String, String, Int, Char, String] = {
        // In some ways this sucks, in other ways it will make it much clearer what each index is.
        val orderID = orderInfo._8.toString
        val customerID = customerInfo._4.toString
        val customerName = customerInfo._1
        val productID = orderInfo._4.toString
        val productName = orderInfo._2
        val productCategory = orderInfo._1
        val paymentType = orderInfo._6
        val qty = orderInfo._5.toString
        val price = orderInfo._3    // In the return statemetn %.2f gives you tailing zeros
        val datetime = timestamp
        val country = customerInfo._3
        val city = customerInfo._2
        val website = orderInfo._7
        val transactionID = currentTransactionID.toString
        val transactionSuccess = success
        val failureReason = reason
        
        return f"$orderID,$customerID,$customerName,$productID,$productName,$productCategory,$paymentType,$qty,$price%.2f,$datetime,$country,$city,$website,$transactionID,$transactionSuccess,$failureReason" 
    }
}
