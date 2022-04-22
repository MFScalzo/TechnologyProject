package ecommerce

import ecommerce.Product
import scala.collection.mutable.ListBuffer
//import scala.compiletime.ops.string

class Order {
    val paymentOptions = Vector("Visa", "Master Card", "Discover", "PayPal", "ApplePay")
    val websites = Vector("www.Amazon.com", "www.BestBuy.com", "www.Walmart.com", "www.Ebay.com", "www.Etsy.com")
    var order_id = 1     
    val productGenerator = new Product()

    def incOrderID(): Unit = {              //Increment Order_ID to make sure unique values are used
        order_id = order_id + 1
    }

    // val productList = Vector(                                        //Sample Products for testing
    //     ("Electronics", "iPhone 13 Pro Max", 1099.00, 1), 
    //     ("Electronics", " 4K Smart TV (2022)", 2199.99, 2), 
    //     ("Electronics", "LG Refridgerator Stainless Steel", 2099, 3),  
    //     ("Electronics", "MacBook Pro Space Gray", 2499.00, 4),
    //     ("Electronics", "Sony - PlayStation 5 Console", 499.00, 5))

    def generateProductList(r: Int): Vector[(String, String, Float, Int, Int)] = {            //Creates a product a random number of times
        var orderProducts = new ListBuffer[Tuple5[String, String, Float, Int, Int]]()             //Will need to know number and kind of entries
        for(i <- 0 to r) {                                              
            var a = scala.util.Random
            var orderQTY = a.nextInt(10) + 1                //Creates random qty
            val product = productGenerator.generateProductInfo     //Creates product + qty
            //orderProducts += (productList(i) + orderQTY.toString)           //Line used for testing
            orderProducts += Tuple5(product._1, product._2, product._3.toFloat, product._4, orderQTY)

        }

        return orderProducts.toVector
    }

    def generateOrderInfo(): Tuple4[Vector[Tuple5[String, String, Float, Int, Int]], String, String, Int] = {           //Generation of one order at a time
        var r = scala.util.Random                  //Random Number of Products

        var products_ordered = generateProductList(r.nextInt(5) + 1)      //Get List of Products + Qty
        var ranPayment = paymentOptions(r.nextInt(paymentOptions.length)).toString    //Select random Payment Type
        var ranWebsite = websites(r.nextInt(websites.length)).toString        //Select random Website       //Select random Website

        val order = Tuple4(products_ordered, ranPayment, ranWebsite, order_id)
        incOrderID()

        return order
    }
}
// object Test extends Order {                             //Code used for testing by itself
    
//     def main(args: Array[String]): Unit = {
//         for(i <- 0 to 3){
//             val order = GenerateOrderInfo()
//             println(order)
//         }
//     }
// }
