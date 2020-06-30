name := "eCommerce-Behavior-Data-Analysis"

version := "0.1"

scalaVersion := "2.11.9"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % "2.4.0" % "provided"),
  ("org.apache.spark" %% "spark-sql" % "2.4.3" % "provided")
)