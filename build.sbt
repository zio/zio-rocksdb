val mainScala = "2.12.12"
val allScala  = Seq(mainScala, "2.13.3")

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.dev/zio-rocksdb/")),
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

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

val zioVersion = "2.0.0"

lazy val rocksdb =
  project
    .in(file("."))
    .settings(
      name := "zio-rocksdb",
      scalafmtOnCompile := true,
      buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion, isSnapshot),
      buildInfoPackage := "zio.rocksdb",
      libraryDependencies ++= Seq(
        "dev.zio"                %% "zio-streams"             % zioVersion,
        "org.scala-lang.modules" %% "scala-collection-compat" % "2.6.0",
        "dev.zio"                %% "zio-test"                % zioVersion % "test",
        "dev.zio"                %% "zio-test-sbt"            % zioVersion % "test",
        "org.rocksdb"            % "rocksdbjni"               % "7.4.5"
      ),
      scalacOptions --= Seq("-Xlint:nullary-override"),
      testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
    )
    .enablePlugins(BuildInfoPlugin)

lazy val docs = project
  .in(file("zio-rocksdb-docs"))
  .settings(
    publish / skip := true,
    moduleName := "zio-rocksdb-docs",
    scalacOptions -= "-Yno-imports",
    scalacOptions -= "-Xfatal-warnings",
    projectName := "ZIO RocksDB",
    mainModuleName := (rocksdb / moduleName).value,
    projectStage := ProjectStage.Development,
    docsPublishBranch := "zio2",
    ScalaUnidoc / unidoc / unidocProjectFilter := inProjects(rocksdb),
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % zioVersion
    ),
    readmeLicense += "\n\nCopyright 2019 Itamar Ravid and the zio-rocksdb contributors. All rights reserved."
  )
  .dependsOn(rocksdb)
  .enablePlugins(WebsitePlugin)
