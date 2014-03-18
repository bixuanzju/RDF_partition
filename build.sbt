name := "Exercise Project"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.0-incubating" % "provided"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.2.1" % "provided"

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-graphx" % "0.9.0-incubating").
    exclude("org.mortbay.jetty", "servlet-api").
    exclude("commons-beanutils", "commons-beanutils-core").
    exclude("commons-collections", "commons-collections").
    exclude("commons-collections", "commons-collections").
    exclude("com.esotericsoftware.minlog", "minlog")
)

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
