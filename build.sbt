name := "scala-tsp"

version := "1.0"

scalaVersion := "3.1.1"

scalacOptions ++= Seq("-deprecation", "-feature")

libraryDependencies += "org.ojalgo" % "ojalgo" % "51.4.0"

libraryDependencies +=
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4"

