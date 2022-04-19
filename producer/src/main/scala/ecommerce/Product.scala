package ecommerce
/*
The Product.scala file is where the products list is managed and includes all product info.
It contains one function in the Product class which returns one random product when called.
*/
class Product {
    def generateProductInfo: Any = {
        val productList = Vector(
        ("Electronics", "iPhone 13 Pro Max", "$1099", 1), 
        ("Electronics", "S95B OLED 4K Smart TV (2022)", "$2199.99", 2), 
        ("Electronics", "LG Eectronics 33in. 25 cu. ft. 3-Door French Door Refridgerator in PrintProof Stainless Steel", "$2099", 3),  
        ("Electronics", "MacBook Pro 14\" Laptop - Apple M1 Pro chip - 16GB Memory - 1TB SSD (Latest ModeL) - Space Gray", "$2499.00", 4),
        ("Electronics", "Sony - PlayStation 5 Console", "$499.00", 5), 
        ("Bedding", "Nestwell Pure Earth Organic Cotton 3-Piece Full/Queen Comforter Set in White", "$170.00", 6), 
        ("Bedding", "Simply Essential™ Adjustable Memory Foam Standard/Queen Bed Pillow", "$15.00", 7), 
        ("Bedding", "Nestwell Supreme Softness Plush Full/Queen Blanket in Pebble Grey", "$55.00", 8), 
        ("Bedding", "Therapedic Waterproof Cotton Queen Mattress Pad", "$99.99", 9), 
        ("Bedding", "Brookstone Perfect 24-Inch Queen Air Mattress in Grey", "$329.99", 10), 
        ("Pet Supplies", "Royal Canin Feline Health Nutrition™ Indoor Adult Cat Food", "$56.99", 11), 
        ("Pet Supplies", "Whisker City 60-in Playbox, Ball Track with Scratcher Toys Cat Tree, Linen", "164.99", 12), 
        ("Pet Supplies", "Chance & Friends \"Zen\" Plush Turtle", "$5.00", 13), 
        ("Pet Supplies", "Purina Pro Plan Complete Essentials Adult Dry Dog Food - High Protein, Probiotics, Lamb & Rice", "$42.99", 14), 
        ("Pet Supplies", "Top Paw Spiky Football Dog Toy - Squeaker", "$2.99", 15)
        )
        
        val productCount = productList.length    
        val r = scala.util.Random
        val current_product = (r.nextInt(productCount) ) //returns a number from 1-n where n is the no. of products in productList.
        val product_info = productList
        product_info(current_product)
    }
}

