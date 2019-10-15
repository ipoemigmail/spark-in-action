import sbt.librarymanagement.ModuleID
import sbt.librarymanagement.syntax._

object Dependencies {

  lazy val kindProjector = "org.typelevel" % "kind-projector" % "0.11.0"
  lazy val catsEffect = "org.typelevel" %% "cats-effect" % "2.0.0"

  lazy val circeBundle: Seq[ModuleID] = Seq(
    "io.circe" %% "circe-core",
    "io.circe" %% "circe-generic",
    "io.circe" %% "circe-generic-extras",
    "io.circe" %% "circe-parser"
  ).map(_ % "0.11.1")

  lazy val sparkBundle: Seq[ModuleID] = Seq(
    "org.apache.spark" %% "spark-yarn",
    "org.apache.spark" %% "spark-core",
    "org.apache.spark" %% "spark-sql",
    "org.apache.spark" %% "spark-mllib",
    "org.apache.spark" %% "spark-streaming",
    "org.apache.spark" %% "spark-hive"
  ).map(_ % "2.4.4")

  lazy val scalaCheck = "org.scalacheck" %% "scalacheck" % "1.14.1"
}
