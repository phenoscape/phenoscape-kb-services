package org.phenoscape.kb

import scala.collection.immutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Right

import org.phenoscape.kb.Term.TermSearchResultsMarshaller
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.httpx.unmarshalling.Deserialized
import spray.httpx.unmarshalling.Deserializer
import spray.json.JsObject
import spray.json.deserializationError
import spray.json.pimpString
import spray.routing.Directive.pimpApply
import spray.routing.SimpleRoutingApp
import spray.routing.directives.ParamDefMagnet.apply

object Main extends App with SimpleRoutingApp with CORSDirectives {

  implicit val system = ActorSystem("phenoscape-kb-system")
  val factory = OWLManager.getOWLDataFactory
  val owlClass = OWLRDFVocabulary.OWL_CLASS.getIRI
  val rdfsLabel = factory.getRDFSLabel.getIRI

  implicit object IRIValue extends Deserializer[String, IRI] {

    def apply(text: String): Deserialized[IRI] = Right(IRI.create(text))

  }

  implicit object IRISeq extends Deserializer[String, Seq[IRI]] {

    def apply(text: String): Deserialized[Seq[IRI]] = Right(text.split(",", -1).map(IRI.create))

  }

  implicit object SimpleMapFromJSONString extends Deserializer[String, Map[String, String]] {

    def apply(text: String): Deserialized[Map[String, String]] = text.parseJson match {
      case o: JsObject => Right(o.fields.map { case (key, value) => key -> value.toString })
      case _ => deserializationError("JSON object expected")
    }

  }

  val conf = ConfigFactory.load()
  val serverPort = conf.getInt("kb-services.port")

  startServer(interface = "localhost", port = serverPort) {

    corsFilter(List("*")) {

      pathPrefix("term") {
        path("search") {
          parameters('text, 'type.as[IRI].?(owlClass), 'property.?(rdfsLabel)) { (text, termType, property) =>
            complete {
              Term.search(text, termType, property)
            }
          }
        } ~
          path("labels") {
            parameters('iris.as[Seq[IRI]]) { (iris) =>
              complete {
                Term.labels(iris: _*)
              }
            }
          }
      } ~
        pathPrefix("characterstate") {
          path("search") {
            parameters('text, 'limit.as[Int]) { (text, limit) =>
              complete {
                CharacterDescription.search(text, limit)
              }
            }
          }
        } ~
        pathPrefix("entity") {
          path("search") {
            parameters('text, 'limit.as[Int]) { (text, limit) =>
              complete {
                Term.searchAnatomicalTerms(text, limit)
              }
            }
          } ~
            pathPrefix("absence") {
              path("evidence") {
                parameters('taxon.as[IRI], 'entity.as[IRI]) { (taxon, entity) =>
                  complete {
                    PresenceAbsenceOfStructure.statesEntailingAbsence(taxon, entity)
                  }
                }
              } ~
                pathEnd {
                  parameters('entity.as[IRI]) { entity =>
                    complete {
                      PresenceAbsenceOfStructure.taxaExhibitingAbsence(entity)
                    }
                  }
                }

            } ~
            pathPrefix("presence") {
              path("evidence") {
                parameters('taxon.as[IRI], 'entity.as[IRI]) { (taxon, entity) =>
                  complete {
                    PresenceAbsenceOfStructure.statesEntailingPresence(taxon, entity)
                  }
                }
              } ~
                pathEnd {
                  parameters('entity.as[IRI]) { entity =>
                    complete {
                      PresenceAbsenceOfStructure.taxaExhibitingPresence(entity)
                    }
                  }
                }
            }
        } ~
        path("genes_expressed_in_structure") {
          parameters('entity.as[IRI]) { entity =>
            complete {
              Gene.expressedWithinStructure(entity)
            }
          }
        } ~
        path("genes_affecting_phenotype") {
          parameters('entity.as[IRI], 'quality.as[IRI]) { (entity, quality) =>
            complete {
              Gene.affectingPhenotype(entity, quality)
            }
          }
        } ~
        pathPrefix("report") {
          path("data_coverage_figure") {
            complete {
              DataCoverageFigureReport.query()
            }
          } ~
            path("data_coverage_figure_any_taxon") {
              complete {
                DataCoverageFigureReportAnyTaxon.query()
              }
            }
        }

    }
  }

}