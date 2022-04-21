package ecommerce

import scala.util.Random

class Customer {
  val Names = List(
    ("John Appleseed"),
    ("Inayah Kaiser"),
    ("Gabriel Lawrence"),
    ("Dora Atkins"),
    ("Fox Combs"),
    ("Quinn Worthington"),
    ("Suhail Southern"),
    ("Maxime Britt"),
    ("Sherri Wise"),
    ("Monique Bradford"),
    ("Brittney Henderson"),
    ("Daisy-Mae Mosley"),
    ("Robson Flores"),
    ("Theo Archer"),
    ("Adrienne Penn")
  )

  val Location = List(
    ("Miami", "United States"),
    ("Boston", "United States"),
    ("New York", "United States"),
    ("New Orleans", "United States"),
    ("Seattle", "United States"),
    ("San Diego", "United States"),
    ("Los Angeles", "Venezuela"),
    ("Chicago", "United States"),
    ("Toronto", "Canada"),
    ("Vancouver", "Canada"),
    ("Montreal", "Canada"),
    ("Ottawa", "Canada"),
    ("Windsor", "Canada"),
    ("Caracas", "Venezuela"),
    ("La Asunción", "Venezuela")
  )

  var nextCustomerID: Int = 1

  def generateCustomerInfo(): Tuple4[String, String, String, Int] = {
    val getName = Names(Random.nextInt(Names.length))
    val getLocation = Location(Random.nextInt(Location.length))
    val customer = (getName, getLocation._1, getLocation._2, nextCustomerID)
    // println(customer)
    nextCustomerID += 1
    return customer
  }
}
