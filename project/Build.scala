import sbt._
import sbtassembly.Plugin._
import sbt.Keys._
import AssemblyKeys._

object HelloakkaBuild extends Build {

  lazy val helloakka = Project(
    id = "helloakka",
    base = file("."),
    settings = Project.defaultSettings ++ assemblySettings ++ Seq(
      name := "helloakka",
      organization := "org.arkajit",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.2",
      resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
      libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.5"
    )
  )
}
