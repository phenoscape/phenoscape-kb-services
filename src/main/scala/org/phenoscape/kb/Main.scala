package org.phenoscape.kb

import scala.collection.immutable.Map
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
import org.phenoscape.kb.OWLFormats.OWLClassExpressionMarshaller
import com.typesafe.config.ConfigFactory
import akka.actor.ActorSystem
import spray.http.HttpHeaders.RawHeader
import spray.http.HttpHeaders.`Cache-Control`
import spray.http.CacheDirectives.`must-revalidate`
import spray.http.CacheDirectives.`max-age`
import spray.http.CacheDirectives.`s-maxage`
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
import spray.http.CacheDirectives
import spray.http.StatusCodes
import scalaz.Success
import scalaz.Failure

object Main extends App with SimpleRoutingApp with CORSDirectives {

  implicit val system = ActorSystem("phenoscape-kb-system")
  import system.dispatcher
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
      respondWithHeaders(
        RawHeader("Vary", "negotiate, Accept"),
        `Cache-Control`(`must-revalidate`, `max-age`(0), `s-maxage`(2592001))) {
          pathPrefix("kb") {
            path("annotation_summary") {
              complete {
                KB.annotationSummary
              }
            } ~
              path("annotation_report") {
                complete {
                  KB.annotationReport
                }
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
                      Term.computedLabel(iri)
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
                path("classification") {
                  parameters('iri.as[IRI]) { (iri) =>
                    complete {
                      Term.classification(iri)
                    }
                  }
                } ~
                path("least_common_subsumers") {
                  parameters('iris.as[Seq[IRI]], 'definedBy.as[IRI].?) { (iris, source) =>
                    complete {
                      Term.leastCommonSubsumers(iris, source)
                    }
                  }
                } ~
                path("all_ancestors") {
                  parameters('iri.as[IRI]) { (iri) =>
                    complete {
                      Term.allAncestors(iri)
                    }
                  }
                } ~
                path("all_descendants") {
                  parameters('iri.as[IRI]) { (iri) =>
                    complete {
                      Term.allDescendants(iri)
                    }
                  }
                } ~
                path("resolve_label_expression") {
                  parameters('expression) { (expression) =>
                    complete {
                      Term.resolveLabelExpression(expression) match {
                        case Success(expression) => expression
                        case Failure(error)      => StatusCodes.UnprocessableEntity -> error
                      }
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
              parameters('entity.as[OWLClassExpression], 'taxon.as[OWLClassExpression], 'variable_only.as[Boolean].?(true), 'parts.as[Boolean].?(false)) { (entity, taxon, variableOnly, includeParts) =>
                respondWithHeader(HttpHeaders.`Content-Disposition`("attachment", Map("filename" -> "ontotrace.xml"))) {
                  complete {
                    PresenceAbsenceOfStructure.presenceAbsenceMatrix(entity, taxon, variableOnly, includeParts)
                  }
                }
              }
            } ~
            pathPrefix("similarity") {
              path("query") {
                parameters('iri.as[IRI], 'corpus_graph.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0)) { (query, corpusGraph, limit, offset) =>
                  complete {
                    Similarity.querySimilarProfiles(query, corpusGraph, limit, offset)
                  }
                }
              } ~
                path("best_matches") {
                  parameters('query_iri.as[IRI], 'corpus_iri.as[IRI], 'query_graph.as[IRI], 'corpus_graph.as[IRI]) { (queryItem, corpusItem, queryGraph, corpusGraph) =>
                    complete {
                      Similarity.bestAnnotationsMatchesForComparison(queryItem, queryGraph, corpusItem, corpusGraph)
                    }
                  }
                } ~
                path("best_subsumers") {
                  parameters('query_iri.as[IRI], 'corpus_iri.as[IRI], 'corpus_graph.as[IRI]) { (queryItem, corpusItem, corpusGraph) =>
                    complete {
                      Similarity.bestSubsumersForComparison(queryItem, corpusItem, corpusGraph)
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
                  parameters('corpus_graph.as[IRI]) { (corpusGraph) =>
                    complete {
                      Similarity.corpusSize(corpusGraph).map(ResultCount(_))
                    }
                  }
                } ~
                path("ic_disparity") {
                  parameters('iri.as[OWLClass], 'queryGraph.as[IRI], 'corpus_graph.as[IRI]) { (term, queryGraph, corpusGraph) =>
                    complete {
                      Similarity.icDisparity(term, queryGraph, corpusGraph).map(value => JsObject("value" -> value.toJson))
                    }
                  }
                }
            } ~
            pathPrefix("characterstate") {
              path("search") { //undocumented
                parameters('text, 'limit.as[Int]) { (text, limit) =>
                  complete {
                    CharacterDescription.search(text, limit)
                  }
                }
              } ~
                path("query") { //undocumented
                  parameters('entity.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'taxon.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (entity, taxon, limit, offset, total) =>
                    complete {
                      if (total) CharacterDescription.queryTotal(entity, taxon, Nil)
                      else CharacterDescription.query(entity, taxon, Nil, limit, offset)
                    }
                  }
                } ~
                path("with_annotation") {
                  parameter('iri.as[IRI]) { iri =>
                    complete {
                      CharacterDescription.annotatedCharacterDescriptionWithAnnotation(iri)
                    }
                  }
                }
            } ~
            pathPrefix("taxon") {
              path("phenotypes") {
                parameters('taxon.as[IRI], 'entity.as[OWLClassExpression].?, 'quality.as[OWLClassExpression].?, 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (taxon, entityOpt, qualityOpt, limit, offset, total) =>
                  complete {
                    if (total) Taxon.directPhenotypesTotalFor(taxon, entityOpt, qualityOpt).map(ResultCount(_))
                    else Taxon.directPhenotypesFor(taxon, entityOpt, qualityOpt, limit, offset)
                  }
                }
              } ~
                path("variation_profile") {
                  parameters('taxon.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (taxon, limit, offset, total) =>
                    complete {
                      if (total) Taxon.variationProfileTotalFor(taxon).map(ResultCount(_))
                      else Taxon.variationProfileFor(taxon, limit, offset)
                    }
                  }
                } ~
                path("with_phenotype") {
                  parameters('entity.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'quality.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'in_taxon.as[IRI].?, 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                    (entity, quality, taxonOpt, limit, offset, total) =>
                      complete {
                        if (total) Taxon.withPhenotypeTotal(entity, quality, taxonOpt).map(ResultCount(_))
                        else Taxon.withPhenotype(entity, quality, taxonOpt, limit, offset)
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
                path("group") {
                  parameters('iri.as[IRI]) { (taxon) =>
                    complete {
                      Taxon.commonGroupFor(taxon)
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
                    parameters('taxon.as[IRI], 'entity.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (taxon, entity, limit, offset, totalOnly) =>
                      complete {
                        if (totalOnly) PresenceAbsenceOfStructure.statesEntailingAbsenceTotal(taxon, entity).map(ResultCount(_))
                        else PresenceAbsenceOfStructure.statesEntailingAbsence(taxon, entity, limit, offset)
                      }
                    }
                  } ~
                    pathEnd {
                      parameters('entity.as[IRI], 'in_taxon.as[IRI].?, 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (entity, taxonFilter, limit, offset, totalOnly) =>
                        complete {
                          if (totalOnly) PresenceAbsenceOfStructure.taxaExhibitingAbsenceTotal(entity, taxonFilter).map(ResultCount(_))
                          else PresenceAbsenceOfStructure.taxaExhibitingAbsence(entity, taxonFilter, limit = limit, offset = offset)
                        }
                      }
                    }

                } ~
                pathPrefix("presence") {
                  path("evidence") {
                    parameters('taxon.as[IRI], 'entity.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (taxon, entity, limit, offset, totalOnly) =>
                      complete {
                        if (totalOnly) PresenceAbsenceOfStructure.statesEntailingPresenceTotal(taxon, entity).map(ResultCount(_))
                        else PresenceAbsenceOfStructure.statesEntailingPresence(taxon, entity, limit, offset)
                      }
                    }
                  } ~
                    pathEnd {
                      parameters('entity.as[IRI], 'in_taxon.as[IRI].?, 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (entity, taxonFilter, limit, offset, totalOnly) =>
                        complete {
                          if (totalOnly) PresenceAbsenceOfStructure.taxaExhibitingPresenceTotal(entity, taxonFilter).map(ResultCount(_))
                          else PresenceAbsenceOfStructure.taxaExhibitingPresence(entity, taxonFilter, limit = limit, offset = offset)
                        }
                      }
                    }
                }
            } ~
            pathPrefix("gene") {
              path("search") {
                parameters('text, 'taxon.as[IRI].?) { (text, taxonOpt) =>
                  complete {
                    Gene.search(text, taxonOpt)
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
                path("expression_profile") {
                  parameters('iri.as[IRI]) { (iri) =>
                    complete {
                      Gene.expressionProfile(iri)
                    }
                  }
                } ~
                path("affecting_entity_phenotype") {
                  parameters('iri.as[IRI], 'parts.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (iri, includeParts, limit, offset, total) =>
                    complete {
                      if (total) Gene.affectingPhenotypeOfEntityTotal(iri, includeParts).map(ResultCount(_))
                      else Gene.affectingPhenotypeOfEntity(iri, includeParts, limit, offset)
                    }
                  }
                } ~
                path("expressed_within_entity") {
                  parameters('iri.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (iri, limit, offset, total) =>
                    complete {
                      if (total) Gene.expressedWithinEntityTotal(iri).map(ResultCount(_))
                      else Gene.expressedWithinEntity(iri, limit, offset)
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
            pathPrefix("study") {
              path("query") {
                parameters('entity.as[OWLClassExpression].?, 'taxon.as[OWLClassExpression].?) { (entity, taxon) =>
                  complete {
                    Study.queryStudies(entity, taxon)
                  }
                }
              } ~
                path("taxa") {
                  parameters('iri.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (studyIRI, limit, offset, total) =>
                    complete {
                      if (total) Study.annotatedTaxaTotal(studyIRI).map(ResultCount(_))
                      else Study.annotatedTaxa(studyIRI, limit, offset)
                    }
                  }
                } ~
                path("phenotypes") {
                  parameters('iri.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (studyIRI, limit, offset, total) =>
                    complete {
                      if (total) Study.annotatedPhenotypesTotal(studyIRI).map(ResultCount(_))
                      else Study.annotatedPhenotypes(studyIRI, limit, offset)
                    }
                  }
                } ~
                path("matrix") {
                  parameters('iri.as[IRI]) { (iri) =>
                    complete {
                      val prettyPrinter = new scala.xml.PrettyPrinter(9999, 2)
                      Study.queryMatrix(iri).map(prettyPrinter.format(_))
                    }
                  }
                } ~
                pathEnd {
                  parameters('iri.as[IRI]) { iri =>
                    complete {
                      Study.withIRI(iri)
                    }
                  }
                }
            } ~
            pathPrefix("phenotype") {
              path("direct_annotations") {
                parameters('iri.as[IRI]) { (iri) =>
                  complete {
                    CharacterDescription.eqAnnotationsForPhenotype(iri)
                  }
                }
              }
            } ~
            pathPrefix("report") {
              path("data_coverage_figure") {
                complete {
                  DataCoverageFigureReport.query()
                }
              } ~
                path("data_coverage_figure_catfish") {
                  complete {
                    DataCoverageFigureReportCatfish.query()
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

}