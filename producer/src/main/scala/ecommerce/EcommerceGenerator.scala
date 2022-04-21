package ecommerce



import scala.util.Random

import ecommerce.{Order, Customer}

class EcommerceGenerator {

    
    val genOrder = new Order()
    val genCustomer = new Customer()



    def incTransaction_Id(): Unit = {             
        incTransaction_Id += 1
    }


    def DateTime():




        val firstYear = 2000
        val lastYear = 2021
        val r =  new scala.util.Random
        val randomYear = firstYear + r.nextInt( (lastYear - firstYear) + 1)

        val firstMonth = 1
        val lastMonth = 12
        val randomMonth = firstMonth + r.nextInt( (lastMonth - firstMonth) + 1)

        val firstDay = 1
        val lastDay = 30
        val randomDay = firstDay + r.nextInt( (lastDay - firstDay) + 1)

        val firstHour = 0
        val lastHour = 23
        val randomHour = firstHour + r.nextInt( (lastHour - firstHour) +1)

        val firstMinute = 0
        val lastMinute = 59
        val randomMinute = firstMinute + r.nextInt( (lastMinute - firstMinute) +1)


        println(s"$randomYear-$randomMonth-$randomDay  $randomHour:$randomMinute")


    




 


    def GenerateData(): (List[(String, String, Int, Int, Char,String)]) ={

    val Data = (genOrder,genCustomer,genDateTime,incTransaction_Id, paymentStatus, failuerReason) 
    }
    val failuerReason = ("reason1", "reason2", "reason4", "reason5")
    val genDateTime = new DateTime()
 val order = (products_ordered, ranPayment, ranWebsite, order_id)




}