# phenoscape-kb-services
Web services application for the Phenoscape RDF knowledgebase.

## Development

### Configuration

Configuration options are set in an `application.conf` file on the classpath. Some settings are specific to 
phenoscape-kb-services, while some are [akka-http settings](https://doc.akka.io/docs/akka-http/current/configuration.html#configuration). 
You can start by coping `src/main/resources/application.conf.example` to `src/main/resources/application.conf`.

### Run

To run the server locally, from the repository root launch `sbt`. At the prompt enter `reStart`. This will start the server. 
To restart after editing code, enter `reStart` again. Stop the server using `reStop`.
