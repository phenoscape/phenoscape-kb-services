package org.phenoscape.kb

import scala.collection.immutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Right
import org.phenoscape.kb.Term.JSONResultItemsMarshaller
import org.phenoscape.kb.Term.JSONResultItemMarshaller
import org.phenoscape.kb.Term.IRIMarshaller
import org.phenoscape.kb.Term.IRIsMarshaller
import org.phenoscape.kb.PhenexDataSet.DataSetMarshaller
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary
import org.phenoscape.kb.OWLFormats.ManchesterSyntaxClassExpression
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import spray.httpx.marshalling._
import spray.httpx.unmarshalling._
import spray.httpx.SprayJsonSupport._
import spray.json._
import spray.json.DefaultJsonProtocol._
import spray.routing._
import spray.routing.SimpleRoutingApp
import spray.routing.directives._
import org.semanticweb.owlapi.model.OWLClassExpression
import org.phenoscape.kb.KBVocab._
import spray.http.HttpHeaders
import org.semanticweb.owlapi.model.OWLClass
import org.semanticweb.owlapi.model.OWLNamedIndividual

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

  implicit object OWLClassValue extends Deserializer[String, OWLClass] {

    def apply(text: String): Deserialized[OWLClass] = Right(factory.getOWLClass(IRI.create(text)))

  }

  implicit object OWLNamedIndividualValue extends Deserializer[String, OWLNamedIndividual] {

    def apply(text: String): Deserialized[OWLNamedIndividual] = Right(factory.getOWLNamedIndividual(IRI.create(text)))

  }

  implicit object SimpleMapFromJSONString extends Deserializer[String, Map[String, String]] {

    def apply(text: String): Deserialized[Map[String, String]] = text.parseJson match {
      case o: JsObject => Right(o.fields.map { case (key, value) => key -> value.toString })
      case _           => deserializationError("JSON object expected")
    }

  }

  implicit object SeqFromJSONString extends Deserializer[String, Seq[String]] {

    def apply(text: String): Deserialized[Seq[String]] = text.parseJson match {
      case a: JsArray => Right(a.elements.map(_.convertTo[String]))
      case _          => deserializationError("JSON array expected")
    }

  }

  val conf = ConfigFactory.load()
  val serverPort = conf.getInt("kb-services.port")

  startServer(interface = "localhost", port = serverPort) {

    corsFilter(List("*")) {
      path("kb" / "annotation_summary") {
        complete {
          KB.annotationSummary
        }
      } ~
        pathPrefix("term") {
          path("search") {
            parameters('text, 'type.as[IRI].?(owlClass), 'property.?(rdfsLabel)) { (text, termType, property) =>
              complete {
                Term.search(text, termType, property)
              }
            }
          } ~
            path("search_classes") {
              parameters('text, 'definedBy.as[IRI], 'limit.as[Int].?(0)) { (text, definedBy, limit) =>
                complete {
                  Term.searchOntologyTerms(text, definedBy, limit)
                }
              }
            } ~
            path("label") {
              parameters('iri.as[IRI]) { (iri) =>
                complete {
                  Term.label(iri).map(_.getOrElse(MinimalTerm(iri, "<unlabeled>")))
                }
              }
            } ~
            path("labels") {
              parameters('iris.as[Seq[IRI]]) { (iris) =>
                complete {
                  Term.labels(iris: _*)
                }
              }
            } ~
            pathEnd {
              parameters('iri.as[IRI]) { iri =>
                complete {
                  Term.withIRI(iri)
                }
              }
            }
        } ~
        path("ontotrace") {
          parameters('entity.as[OWLClassExpression], 'taxon.as[OWLClassExpression], 'variable_only.as[Boolean].?(true)) { (entity, taxon, variableOnly) =>
            respondWithHeader(HttpHeaders.`Content-Disposition`("attachment", Map("filename" -> "ontotrace.xml"))) {
              complete {
                PresenceAbsenceOfStructure.presenceAbsenceMatrix(entity, taxon, variableOnly)
              }
            }
          }
        } ~
        pathPrefix("similarity") {
          path("query") {
            parameters('iri.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0)) { (query, limit, offset) =>
              complete {
                Similarity.evolutionaryProfilesSimilarToGene(query, limit, offset)
              }
            }
          } ~
            path("best_matches") {
              parameters('query_iri.as[IRI], 'corpus_iri.as[IRI]) { (queryItem, corpusItem) =>
                complete {
                  Similarity.bestAnnotationsMatchesForComparison(queryItem, corpusItem)
                }
              }
            } ~
            path("best_subsumers") {
              parameters('query_iri.as[IRI], 'corpus_iri.as[IRI]) { (queryItem, corpusItem) =>
                complete {
                  Similarity.bestSubsumersForComparison(queryItem, corpusItem)
                }
              }
            } ~
            path("subsumed_annotations") {
              parameters('subsumer.as[OWLClass], 'instance.as[OWLNamedIndividual]) { (subsumer, instance) =>
                complete {
                  Similarity.subsumedAnnotations(instance, subsumer)
                }
              }
            } ~
            path("profile_size") {
              parameters('iri.as[IRI]) { (iri) =>
                complete {
                  Similarity.profileSize(iri).map(ResultCount(_))
                }
              }
            } ~
            path("corpus_size") {
              complete {
                Similarity.corpusSize.map(ResultCount(_))
              }
            }
        } ~
        path("ic_disparity") {
          parameters('iri.as[OWLClass]) { (term) =>
            complete {
              Similarity.icDisparity(term).map(value => JsObject("value" -> value.toJson))
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
          } ~
            path("query") {
              parameters('entity.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'taxon.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (entity, taxon, limit, offset, total) =>
                complete {
                  if (total) CharacterDescription.queryTotal(entity, taxon, Nil)
                  else CharacterDescription.query(entity, taxon, Nil, limit, offset)
                }
              }
            }
        } ~
        pathPrefix("taxon") {
          path("query") {
            parameters('entity.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'taxon.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (entity, taxon, limit, offset, total) =>
              complete {
                if (total) Taxon.queryTotal(entity, taxon, Nil)
                else Taxon.query(entity, taxon, Nil, limit, offset)

              }
            }
          } ~
            path("variation_profile") {
              parameters('taxon.as[Seq[String]].?(Seq.empty[String]), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (taxa, limit, offset, total) =>
                complete {
                  if (total) CharacterDescription.queryVariationProfileTotal(taxa.map(IRI.create)).map(ResultCount(_))
                  else CharacterDescription.queryVariationProfile(taxa.map(IRI.create), limit, offset)
                }
              }
            } ~
            path("newick") {
              parameters('iri.as[IRI]) { (taxon) =>
                complete {
                  Taxon.newickTreeWithRoot(taxon)
                }
              }
            } ~
            pathEnd {
              parameters('iri.as[IRI]) { iri =>
                complete {
                  Taxon.withIRI(iri)
                }
              }
            }
        } ~
        pathPrefix("entity") {
          path("search") {
            parameters('text, 'limit.as[Int]) { (text, limit) =>
              complete {
                Term.searchOntologyTerms(text, Uberon, limit)
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
                  parameters('entity.as[IRI], 'limit.as[Int]) { (entity, limit) =>
                    complete {
                      PresenceAbsenceOfStructure.taxaExhibitingAbsence(entity, limit)
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
                  parameters('entity.as[IRI], 'limit.as[Int]) { (entity, limit) =>
                    complete {
                      PresenceAbsenceOfStructure.taxaExhibitingPresence(entity, limit)
                    }
                  }
                }
            }
        } ~
        pathPrefix("gene") {
          path("search") {
            parameters('text) { (text) =>
              complete {
                Gene.search(text)
              }
            }
          } ~
            path("eq") {
              parameters('id.as[IRI]) { iri =>
                complete {
                  EQForGene.query(iri)
                }
              }
            } ~
            path("query") {
              parameters('entity.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'taxon.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (entity, taxon, limit, offset, total) =>
                complete {
                  if (total) Gene.queryTotal(entity, taxon)
                  else Gene.query(entity, taxon, limit, offset)
                }
              }
            } ~
            path("phenotypic_profile") {
              parameters('iri.as[IRI]) { (iri) =>
                complete {
                  Gene.phenotypicProfile(iri)
                }
              }
            } ~
            pathEnd {
              parameters('iri.as[IRI]) { iri =>
                complete {
                  Gene.withIRI(iri)
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