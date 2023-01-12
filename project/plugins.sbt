addSbtPlugin("org.scalameta" % "sbt-scalafmt"    % "2.2.1")
addSbtPlugin("com.eed3si9n"  % "sbt-buildinfo"   % "0.9.0")
addSbtPlugin("com.geirsson"  % "sbt-ci-release"  % "1.5.5")
addSbtPlugin("dev.zio"       % "zio-sbt-website" % "0.3.4")

resolvers ++= Resolver.sonatypeOssRepos("snapshots")
