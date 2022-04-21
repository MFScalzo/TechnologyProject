package ecommerce



import scala.util.Random

import ecommerce.{Order, Customer}

object EcommerceGenerator {
    val genOrder = new Order()
    val genCustomer = new Customer()
    var currentTransactionID = 1
    val failuerReason = ("reason1", "reason2", "reason4", "reason5")
    
    def main(args: Array[String]) {

    }

    def incTransaction_Id(): Unit = {             
        currentTransactionID += 1
    }

    def DateTime(): String = {
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

        return s"$randomYear-$randomMonth-$randomDay  $randomHour:$randomMinute"
    }

    def GenerateData(customerInfo: Tuple4[String, String, String, Int], orderInfo: Tuple8[String, String, Float, Int, Int, String, String, Int]): Tuple16[Int, Int, String, Int, String, String, String, Int, Float, String, String, String, String, Int, Char, String] = {
        val Data = (orderID, customerID, customerName, productID, productCategory, paymentType, qty, price, datetime, country, city, webite, transactionID, transactionSuccess, failureReason) 
    }
    
    val order = (products_ordered, ranPayment, ranWebsite, order_id)

}