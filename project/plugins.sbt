addSbtPlugin("org.scalameta" % "sbt-scalafmt"    % "2.2.1")
addSbtPlugin("com.eed3si9n"  % "sbt-buildinfo"   % "0.9.0")
addSbtPlugin("com.geirsson"  % "sbt-ci-release"  % "1.5.5")
addSbtPlugin("dev.zio"       % "zio-sbt-website" % "0.0.0+84-6fd7d64e-SNAPSHOT")

resolvers ++= Resolver.sonatypeOssRepos("snapshots")
