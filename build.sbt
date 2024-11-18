val mainScala = "2.12.20"
val allScala  = Seq(mainScala, "2.13.15", "3.3.4")

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://github.com/zio/zio-rocksdb")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    useCoursier := false,
    scalaVersion := mainScala,
    crossScalaVersions := allScala,
    Test / parallelExecution := false,
    Test / fork := true,
    run / fork := true,
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    scmInfo := Some(
      ScmInfo(url("https://github.com/zio/zio-rocksdb/"), "scm:git:git@github.com:zio/zio-rocksdb.git")
    ),
    developers := List(
      Developer(
        "iravid",
        "Itamar Ravid",
        "iravid@iravid.com",
        url("https://github.com/iravid")
      )
    )
  )
)

name := "zio-rocksdb"
scalafmtOnCompile := true

enablePlugins(BuildInfoPlugin)
buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, isSnapshot)
buildInfoPackage := "zio.rocksdb"

val zioVersion = "2.1.11"

libraryDependencies ++= Seq(
  "dev.zio"                %% "zio-streams"             % zioVersion,
  "org.scala-lang.modules" %% "scala-collection-compat" % "2.12.0",
  "dev.zio"                %% "zio-test"                % zioVersion % "test",
  "dev.zio"                %% "zio-test-sbt"            % zioVersion % "test",
  "org.rocksdb"            % "rocksdbjni"               % "9.7.3"
)

scalacOptions --= Seq("-Xlint:nullary-override")

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")
