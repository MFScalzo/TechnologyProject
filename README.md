# Technology Project

## Project Description
This project had to main parts, the Producer and the Consumer. The Producer generates random data that resembles transaction from a ecommerce site, saves it to a CSV, and then uploads it to HDFS and Hive. The Consumer takes the generated data and provides meaningful analysis options using Hive and Spark.

## Technologies Used

## Features

## Getting Started
First Clone this repository:

    clone https://github.com/MFScalzo/TechnologyProject.git
    
In VS Code, make sure you have the metals extension. Open which ever program you want to build:

To open and build Producer, open the `producer` directory in VS Code's Open Folder Option

To open and build Consumer, open the `consumer` directory in VS Code's Open Folder Option

Then in the sbt shell build the .jar file:

    package
    
This will create the .jar in the `target/scala-2.11/` folder.

Move this file over to your VM with SCP:

    scp -P 2222 maria_dev@127.0.0.1:~/ecommerce/vanquish

## Usage
Move these files over to your Hortonworks Sandbox HDP 2.6.5 VM and into a folder structure like this: `/home/maria_dev/ecommerce/vanquish/`. To run the use `spark-submit <.jar file> --class ecommerce.ecommerce`. Run the Producer first then the Consumer for desired results.

## Contributors
 - Matthew Scalzo
 - Javier Zapata
 - David Tennessee
 - Drake Tubbs
 - Nicholas Young


## License
This project uses the following license: [MIT License](https://github.com/MFScalzo/TechnologyProject/blob/main/LICENSE.txt)
