name := "DataFusionPipeline"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7",
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.apache.spark" %% "spark-mllib" % "2.4.7",
  "org.postgresql" % "postgresql" % "42.3.1",
  "com.typesafe" % "config" % "1.4.2"
)

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oD")


import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.MergeStrategy

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*)       => MergeStrategy.discard
  case "reference.conf"                    => MergeStrategy.concat
  case "application.conf"                  => MergeStrategy.concat
  case "META-INF/services/org.apache.hadoop" => MergeStrategy.filterDistinctLines
  case "META-INF/services/org.apache.spark"  => MergeStrategy.filterDistinctLines
  case "META-INF/services/org.apache.yarn"   => MergeStrategy.filterDistinctLines
  case "META-INF/io.netty.versions.properties" => MergeStrategy.first
  case x if x.endsWith("pom.properties")   => MergeStrategy.discard
  case x if x.endsWith("pom.xml")          => MergeStrategy.discard
  case x if x.endsWith("git.properties")   => MergeStrategy.first
  case x if x.endsWith("javax.inject.Named.class") => MergeStrategy.first
  case x if x.endsWith("javax.inject.Inject.class") => MergeStrategy.first
  case x if x.endsWith("org/aopalliance/aop/Advice.class") => MergeStrategy.first
  case x if x.endsWith("org/apache/spark/unused/UnusedStubClass.class") => MergeStrategy.first
  case x if x.endsWith("aopalliance/intercept/Interceptor.class") => MergeStrategy.first
  case x if x.endsWith("aopalliance/intercept/MethodInterceptor.class") => MergeStrategy.first
  case x if x.endsWith("org/apache/commons/collections/ArrayStack.class") => MergeStrategy.first
  case x if x.endsWith("org/apache/hadoop/yarn/factories/package-info.class") => MergeStrategy.first
  case x if x.endsWith("org/apache/hadoop/yarn/factory/providers/package-info.class") => MergeStrategy.first
  case x if x.endsWith("org/apache/hadoop/yarn/util/package-info.class") => MergeStrategy.first
  case x => MergeStrategy.first
}