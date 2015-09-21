name := "spark-fim"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.1"
libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.1.3" % "test"

// Spark tests cannot be run in parallel
fork in Test := true

// show compilation warnings
scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")
