# Technology Project

## Project Description
This project has two main programs: the Producer and the Consumer. The Producer generates randomized data that resembles transactions from an eCommerce site, saves this data to a CSV, and uploads it to HDFS and Hive. The Consumer works in conjunction with a CLI (Command Line Interface) application to grab the generated data and provide meaningful analysis options through queries using Hive and Spark.

## Technologies Used
- Scala - version 2.11.8
- Hadoop - version 2.6.5
- Hive - version 2.4.8
- Spark - version 2.4.3
- SparkSQL - version 2.4.3
- gitSCM - version 2.35.1 (+ Github)

## Features
- Produce randomly generated eCommerce data
- Save generated data into a CSV file
- Load generated data into Hive and HDFS
- Operate the Consumer via a CLI (Command Line Interface) menu 
- Perform queries and analysis with Hive and Spark on the data that is stored in HDFS and Hive

## Getting Started
Clone this repository:

    git clone https://github.com/MFScalzo/TechnologyProject.git
    
In VS Code, be sure to have:
- The Scala (Metals) extension by Scalameta
- The Scala (sbt) extension by Lightbend 

Open the program you want to build:

- To open and build Producer, open the `producer` directory using the Open Folder Option in VS Code

- To open and build Consumer, open the `consumer` directory using the Open Folder Option in VS Code

In the sbt shell:

- Compile using:

        compile

- Build the .jar file:

        package
    
This will create the dedicated .jar in the `target/scala-2.11/` folder.

Create necessary folder structure in your Hortonworks Sandbox HDP 2.6.5 VM:

    ssh -P 2222 maria_dev@127.0.0.1
    cd ~
    mkdir -p ecommerce/tech_project

Move this file over to your Hortonworks Sandbox HDP 2.6.5 VM with SCP:

    scp -P 2222 <file here> maria_dev@127.0.0.1:~/ecommerce/tech_project

## Usage
- Navigate to the directory above:
    
        cd ~/ecommerce/tech_project/
    
- To run the Producer, use:
        
        spark-submit <producer file .jar>
 
- To run the Consumer, use:

        spark-submit <consumer file .jar>
        
- Note: The Producer <ins>must</ins> be run before the Consumer for desired results.

## Contributors
 - Matthew Scalzo
 - David Tennessee
 - Drake Tubbs
 - Nicholas Young
 - Javier Zapata


## License
This project uses the following license: [MIT License](https://github.com/MFScalzo/TechnologyProject/blob/main/LICENSE.txt)
