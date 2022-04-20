package ecommerce
/*
The Product.scala file is where the products list is managed and includes all product info.
It contains one function in the Product class which returns one random product when called.
*/
class Product {
    def generateProductInfo: Any = {
        val productList = Vector(
        ("Electronics", "iPhone 13 Pro Max", 1099.00f, 1), 
        ("Electronics", "S95B OLED 4K Smart TV (2022)", 2199.99f, 2), 
        ("Electronics", "LG Eectronics 3-Door French Door Refridgerator", 2099.00f, 3),  
        ("Electronics", "MacBook Pro 14\" Laptop", 2499.00f, 4),
        ("Electronics", "Sony - PlayStation 5 Console", 499.00f, 5), 
        ("Bedding", "Organic Cotton 3-Piece Full/Queen Comforter Set", 170.00f, 6), 
        ("Bedding", "Adjustable Memory Foam Standard/Queen Bed Pillow", 15.00f, 7), 
        ("Bedding", "Supreme Softness Plush Full/Queen Blanket", 55.00f, 8), 
        ("Bedding", "Therapedic Waterproof Cotton Queen Mattress Pad", 99.99f, 9), 
        ("Bedding", "Perfect 24-Inch Queen Air Mattress", 329.99f, 10), 
        ("Pet Supplies", "Royal Canin Indoor Adult Cat Food", 56.99f, 11), 
        ("Pet Supplies", "Whisker City 60-in Cat Tree", 164.99f, 12), 
        ("Pet Supplies", "Chance & Friends \"Zen\" Plush Turtle Toy", 5.00f, 13), 
        ("Pet Supplies", "Purina Adult Dry Dog Food - High Protein", 42.99f, 14), 
        ("Pet Supplies", "Spiky Football Dog Toy - Squeaker", 2.99f, 15)
        )
           
        val r = scala.util.Random
        val current_product = r.nextInt(productList.length) //returns a number from 0-n where n is the no. of products in productList.
        productList(current_product)
    }
}

