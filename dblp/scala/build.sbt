import MergeStrategy._

name := "DBLP-Scala"
version := "0.9"
// This is what's running on IBM A1.2: Scala 2.11, Spark 2.3.2
scalaVersion := "2.11.8"
//val sparkVersion = "2.3.2"
// This is my setup at home (Scala 2.12.8, spark 2.4.0)
//scalaVersion := "2.12.8"
val sparkVersion = "2.4.0"
scalacOptions += "-target:jvm-1.8"
fork in Test := true
parallelExecution in Test := false
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")
testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")
libraryDependencies ++= Seq(
  // These need to be 'provided' for assembly but compiled in for 'run'
  //"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  //"org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  "org.scala-lang" % "scala-library" % "2.11.8",
  "org.scala-lang" % "scala-compiler" % "2.11.8",
  "org.scala-lang" % "scala-reflect" % "2.11.8",

  // Storage locator for talking to S3 on IBM
  // "com.ibm.stocator" % "stocator" % "1.0.1",
  // "org.scalatest" %% "scalatest" % "3.2.0-SNAP9" % Test
  // At least some scalatest 3.2.x snapshots didn't work with scala 2.11.
  "org.scalatest" %% "scalatest" % "2.2.2" % Test
)

/* without this explicit merge strategy code you get a lot of noise from sbt-assembly 
   complaining about not being able to dedup files */
assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case "git.properties" =>MergeStrategy.discard // why does this even get looked at?
  case "overview.html" => MergeStrategy.last  // Added this for 2.1.0 I think
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

/* including scala bloats your assembly jar unnecessarily, and may interfere with 
   spark runtime */
assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false) 

/* you need to be able to undo the "provided" annotation on the deps when running your spark 
   programs locally i.e. from sbt; this bit reincludes the full classpaths in the compile and run tasks. */
fullClasspath in Runtime := (fullClasspath in (Compile, run)).value 

//Disable test in assembly phase.
test in assembly := {}
