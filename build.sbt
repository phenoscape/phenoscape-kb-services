import com.typesafe.sbt.SbtNativePackager._
import NativePackagerKeys._

organization  := "org.phenoscape"

name          := "phenoscape-kb-services"

version       := "0.3"

packageArchetype.java_server

packageDescription in Debian := "Phenoscape KB services"

maintainer in Debian := "James Balhoff"

maintainer in Linux := "James Balhoff <balhoff@nescent.org>"

packageSummary in Linux := "Phenoscape KB services"

packageDescription := "A web api for the Phenoscape Knowledgebase"

daemonUser in Linux := "phenoscape" // user which will execute the application

daemonGroup in Linux := "phenoscape"    // group which will execute the application

scalaVersion  := "2.11.6"

scalacOptions := Seq("-unchecked", "-deprecation", "-feature", "-encoding", "utf8")

resolvers += "Phenoscape Maven repository" at "http://phenoscape.svn.sourceforge.net/svnroot/phenoscape/trunk/maven/repository"

resolvers += "Bigdata repository" at "http://www.systap.com/maven/releases"

resolvers += "NXParser repository" at "http://nxparser.googlecode.com/svn/repository"

resolvers += "BBOP repository" at "http://code.berkeleybop.org/maven/repository"

libraryDependencies ++= {
  val akkaV = "2.3.6"
  val sprayV = "1.3.2"
  Seq(
    "io.spray"               %%  "spray-can"           % sprayV,
    "io.spray"               %%  "spray-routing"       % sprayV,
    "io.spray"               %%  "spray-client"        % sprayV,
    "io.spray"               %%  "spray-json"          % "1.3.1",
    "io.spray"               %%  "spray-testkit"       % sprayV  % "test",
    "com.typesafe.akka"      %%  "akka-actor"          % akkaV,
    "com.typesafe.akka"      %%  "akka-testkit"        % akkaV   % "test",
    "net.sourceforge.owlapi" %   "owlapi-distribution" % "3.5.0",
    "org.phenoscape"         %   "owlet"               % "1.3",
    "org.phenoscape"         %   "scowl"               % "0.9",
    "org.phenoscape"         %%   "kb-owl-tools"       % "1.2",
    "org.phenoscape"         %   "phenex"              % "1.15.4",
    "commons-io"             %   "commons-io"          % "2.4",
    "org.apache.jena"        %   "apache-jena-libs"    % "2.11.2",
    "com.google.guava"       %   "guava"               % "16.0.1",
    "org.scalaz"             %%  "scalaz-core"         % "7.1.1",
    "org.bbop"               %   "oboformat"           % "0.5.5"
  )
}

Revolver.settings
