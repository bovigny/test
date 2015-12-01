name := "QueryMolecules"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "1.4.1",
  "org.apache.spark" % "spark-hive_2.11" % "1.4.1",
  "org.apache.spark" % "spark-sql_2.11" % "1.4.1",
  "com.google.guava" % "guava-collections" % "r03"

)


resolvers += "Akka Repository" at "http://repo.akka.io/releases/"