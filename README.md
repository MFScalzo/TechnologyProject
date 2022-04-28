# Technology Project


## __Producer__

The __Producer__ generates a random data that resembles transaction from a ecommerce site, and uploads it to HDFS and Hive.

## __Consumer__

The __Consumer__ takes the generated data and provides meaningful analysis options using Hive and Spark.

## Usage

First Clone this repository:

    clone https://github.com/MFScalzo/TechnologyProject.git

Then in VS Code, make sure you have the metals extension. Open which ever program you want to build:

To open and build Producer, open the `producer` directory in VS Code's Open Folder Option

To open and build Consumer, open the `consumer` directory in VS Code's Open Folder Option

Then in the sbt shell build the .jar file:

    package
    
This will create the .jar in the `target/scala-2.11/` folder.

Move this file over to your VM with SCP:

    scp -P 2222 maria_dev@127.0.0.1:~/ecommerce/vanquish

These programs were tested in VS Code using the Metals extension. In order to build, you will need to mount each executables parent project folder (`producer` and `consumer`) and run the `pacakge` command in the sbt shell on both of them individually. This will give you two .jar files in `producer/target/scala-2.11` and `consumer/target/scala-2.11`. Move these files over to your Hortonworks Sandbox HDP 2.6.5 VM and into a folder structure like this: `/home/maria_dev/ecommerce/vanquish/`. To run the use `spark-submit <.jar file> --class ecommerce.ecommerce`. Run the Producer first then the Consumer for desired results.
