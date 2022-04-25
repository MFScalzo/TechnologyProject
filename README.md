# Technology Project


## __Producer__

The __Producer__ generates a random data that resembles transaction from a ecommerce site, and uploads it to HDFS and Hive.

## __Consumer__

The __Consumer__ takes the generated data and provides meaningful analysis options using Hive and Spark.

## Usage

These programs were tested in VS Code using the Metals extension. In order to build, you will need to mount each executables parent project folder (`producer` and `consumer`) and run the `pacakge` command in the sbt shell on both of them individually. This will give you two .jar files in `producer/target/scala-2.11` and `consumer/target/scala-2.11`. Move these files over to your Hortonworks Sandbox HDP 2.6.5 VM and into a folder structure like this: `home/maria_dev/ecommerce/vanquish/`. To run the use `spark-submit <.jar file> --class ecommerce.ecommerce`. Run the Producer first then the Consumer for desired results.
