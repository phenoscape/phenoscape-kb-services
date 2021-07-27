enablePlugins(JavaServerAppPackaging)

organization := "org.phenoscape"

name := "phenoscape-kb-services"

version := "0.5"

packageDescription in Debian := "Phenoscape KB services"

maintainer in Debian := "James Balhoff"

maintainer in Linux := "James Balhoff <balhoff@renci.org>"

packageSummary in Linux := "Phenoscape KB services"

packageDescription := "A web api for the Phenoscape Knowledgebase"

daemonUser in Linux := "phenoscape" // user which will execute the application

daemonGroup in Linux := "phenoscape" // group which will execute the application

scalaVersion  := "2.13.6"

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8")

resolvers += "Phenoscape Maven repository" at "https://svn.code.sf.net/p/phenoscape/code/trunk/maven/repository"

resolvers += ("BBOP repository" at "http://code.berkeleybop.org/maven/repository").withAllowInsecureProtocol(true)

testFrameworks += new TestFramework("utest.runner.Framework")

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka"      %% "akka-stream"             % "2.6.12",
    "com.typesafe.akka"      %% "akka-actor"              % "2.6.12",
    "com.typesafe.akka"      %% "akka-slf4j"              % "2.6.12",
    "com.typesafe.akka"      %% "akka-http"               % "10.2.4",
    "com.typesafe.akka"      %% "akka-http-caching"       % "10.2.4",
    "com.typesafe.akka"      %% "akka-http-spray-json"    % "10.2.4",
    "ch.megard"              %% "akka-http-cors"          % "1.0.0",
    "com.lightbend.akka"     %% "akka-stream-alpakka-xml" % "2.0.2",
    "io.spray"               %% "spray-json"              % "1.3.6",
    "net.sourceforge.owlapi" %  "owlapi-distribution"     % "4.5.18",
    "org.phenoscape"         %% "owlet"                   % "1.8.1" exclude("org.slf4j", "slf4j-log4j12"), // exclude("log4j", "log4j"),
    "org.phenoscape"         %% "scowl"                   % "1.4.0",
    "org.phenoscape"         %% "kb-owl-tools"            % "1.13.0" exclude("org.slf4j", "slf4j-log4j12"), // exclude("log4j", "log4j"),
    "org.phenoscape"         %% "phenoscape-kb-ingest"    % "1.6.2",
    "org.phenoscape"         %  "phenex"                  % "1.17.2" exclude("org.slf4j", "slf4j-log4j12") exclude("net.sourceforge.owlapi", "owlapi-apibinding"),
    "commons-io"             %  "commons-io"              % "2.11.0", // exclude("log4j", "log4j"),
    "org.apache.jena"        %  "apache-jena-libs"        % "3.17.0" exclude("org.slf4j", "slf4j-log4j12"),
    "org.scalaz"             %% "scalaz-core"             % "7.2.33",
    "org.bbop"               %  "oboformat"               % "0.5.5" exclude("net.sourceforge.owlapi", "owlapi-apibinding"), // exclude("log4j", "log4j"),
    "ch.qos.logback"         %  "logback-classic"         % "1.2.5",
    "org.codehaus.groovy"    %  "groovy-all"              % "3.0.5",
    "org.phenoscape"         %% "sparql-utils"            % "1.3.1",
    "org.phenoscape"         %% "sparql-utils-owlapi"     % "1.3.1",
    "com.lihaoyi"            %% "utest"                   % "0.7.10" % Test
  )
}

Revolver.settings
