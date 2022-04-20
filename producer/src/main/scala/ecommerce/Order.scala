package ecommerce

import ecommerce.Product
import scala.collection.mutable.ListBuffer
//import scala.compiletime.ops.string

class Order {
    val payment = ("Visa", "Master Card", "Discover", "PayPal", "ApplePay")
    val web = ("www.Amazon.com", "www.BestBuy.com", "www.Walmart.com", "www.Ebay.com", "www.Etsy.com")
    var order_id = 1     
    val productGenerator = new Product()

    def IncOrderID(): Unit = {              //Increment Order_ID to make sure unique values are used
        order_id = order_id + 1
    }

    // val productList = Vector(                                        //Sample Products for testing
    //     ("Electronics", "iPhone 13 Pro Max", 1099.00, 1), 
    //     ("Electronics", " 4K Smart TV (2022)", 2199.99, 2), 
    //     ("Electronics", "LG Refridgerator Stainless Steel", 2099, 3),  
    //     ("Electronics", "MacBook Pro Space Gray", 2499.00, 4),
    //     ("Electronics", "Sony - PlayStation 5 Console", 499.00, 5))

    def GenerateProductList(r: Int): List[String] = {            //Creates a product a random number of times
        var orderProducts = new ListBuffer[String]()             //Will need to know number and kind of entries
        for(i <- 0 to r) {                                              
            var a = scala.util.Random
            var orderQTY = a.nextInt(10) + 1                //Creates random qty
            orderProducts += (productGenerator.generateProductInfo() + orderQTY.toString)     //Creates product + qty
            //orderProducts += (productList(i) + orderQTY.toString)           //Line used for testing
        }
        var order_Products = orderProducts.toList
        return order_Products
    }

    def GenerateOrderInfo(): (List[String], Any, Any, Int) = {           //Generation of one order at a time
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
// object Test extends Order {                             //Code used for testing by itself
    
//     def main(args: Array[String]): Unit = {
//         for(i <- 0 to 3){
//             val order = GenerateOrderInfo()
//             println(order)
//         }
//     }
// }
