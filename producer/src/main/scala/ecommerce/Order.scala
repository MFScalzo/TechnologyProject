package ecommerce

import ecommerce.Product
import scala.collection.mutable.ListBuffer

class Order {
    val payment = ("Visa", "Master Card", "Discover", "PayPal", "ApplePay")
    val web = ("Amazon", "BestBuy", "Walmart", "Ebay", "Etsy")
    var order_id = 1     
    val productGenerator = new Product()

    def IncOrderID(): Unit = {              //Increment Order_ID to make sure unique values are used
        order_id = order_id + 1
    }

    def GenerateProductList(r: Int): List[(String, String, Float, Int, Int)] = {            //Creates a product a random number of times
        var orderProducts = new ListBuffer[(String, String, Float, Int, Int)]()             //Will need to know number and kind of entries
        for(i <- 0 to r) {                                              
            var a = scala.util.Random
            var orderQTY = a.nextInt(10) + 1                //Creates random qty
            orderProducts += (productGenerator.generateProductInfo() + orderQTY)     //Creates product + qty
        }
        var order_Products = orderProducts.toList
        return order_Products
    }

    def GenerateOrderInfo(): (List[(String, String, Float, Int, Int)], Any, Any, Int) = {           //Generation of one order at a time
        var r = scala.util.Random                  //Random Number of Products
        var products_ordered = GenerateProductList(r.nextInt(5))      //Get List of Products + Qty
        var x = scala.util.Random
        var y = scala.util.Random
        var ranPayment = payment.productElement(x.nextInt(5))     //Select random Payment Type
        var ranWebsite = web.productElement(y.nextInt(5))        //Select random Website
        val order = (products_ordered, ranPayment, ranWebsite, order_id)
        IncOrderID()
        return order
    }
}