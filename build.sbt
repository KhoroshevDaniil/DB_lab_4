ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "BD_lab_4"
  )

libraryDependencies += "org.apache.zookeeper" % "zookeeper" % "3.4.13" % "compile"