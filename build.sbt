import com.typesafe.sbt.SbtNativePackager._
import NativePackagerKeys._

organization  := "org.phenoscape"

name          := "phenoscape-kb-services"

version       := "0.5"

packageArchetype.java_server

packageDescription in Debian := "Phenoscape KB services"

maintainer in Debian := "James Balhoff"

maintainer in Linux := "James Balhoff <balhoff@renci.org>"

packageSummary in Linux := "Phenoscape KB services"

packageDescription := "A web api for the Phenoscape Knowledgebase"

daemonUser in Linux := "phenoscape" // user which will execute the application

daemonGroup in Linux := "phenoscape"    // group which will execute the application

scalaVersion  := "2.12.4"

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8")

resolvers += "Phenoscape Maven repository" at "http://phenoscape.svn.sourceforge.net/svnroot/phenoscape/trunk/maven/repository"

resolvers += "BBOP repository" at "http://code.berkeleybop.org/maven/repository"

libraryDependencies ++= {
  Seq(
    "com.typesafe.akka"      %% "akka-stream"          % "2.5.4",
    "com.typesafe.akka"      %% "akka-actor"           % "2.5.4",
    "com.typesafe.akka"      %% "akka-http"            % "10.0.10",
    "com.typesafe.akka"      %% "akka-http-spray-json" % "10.0.10",
    "ch.megard"              %% "akka-http-cors"       % "0.2.1",
    "io.spray"               %% "spray-json"           % "1.3.3",
    "com.typesafe.akka"      %% "akka-slf4j"           % "2.5.4",
    "net.sourceforge.owlapi" %  "owlapi-distribution"  % "4.2.8",
    "org.phenoscape"         %% "owlet"                % "1.6" exclude("org.slf4j", "slf4j-log4j12"), // exclude("log4j", "log4j"),
    "org.phenoscape"         %% "scowl"                % "1.3",
    "org.phenoscape"         %% "kb-owl-tools"         % "1.5.1" exclude("org.slf4j", "slf4j-log4j12"), // exclude("log4j", "log4j"),
    "org.phenoscape"         %% "phenoscape-kb-ingest" % "1.5.1",
    "org.phenoscape"         %  "phenex"               % "1.17.2" exclude("org.slf4j", "slf4j-log4j12") exclude("net.sourceforge.owlapi", "owlapi-apibinding"),
    "commons-io"             %  "commons-io"           % "2.4", // exclude("log4j", "log4j"),
    "org.apache.jena"        %  "apache-jena-libs"     % "3.2.0" exclude("org.slf4j", "slf4j-log4j12"),
    "com.google.guava"       %  "guava"                % "16.0.1",
    "org.scalaz"             %% "scalaz-core"          % "7.2.16",
    "org.bbop"               %  "oboformat"            % "0.5.5" exclude("net.sourceforge.owlapi", "owlapi-apibinding"), // exclude("log4j", "log4j"),
    "ch.qos.logback"         %  "logback-classic"      % "1.1.7",
    "org.codehaus.groovy"    %  "groovy-all"           % "2.4.6",
    "org.phenoscape"         %% "sparql-interpolator"  % "1.0"
  )
}

Revolver.settings
