import Dependencies._

ThisBuild / scalaVersion     := "2.11.8"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.vanquish"
ThisBuild / organizationName := "vanquish"

lazy val root = (project in file("."))
  .settings(
    name := "Producer",
    libraryDependencies += scalaTest % Test,
    libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.0"
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
