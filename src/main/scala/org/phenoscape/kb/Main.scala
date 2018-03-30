package org.phenoscape.kb

import org.apache.jena.system.JenaSystem
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.OWLFormats.ManchesterSyntaxClassExpressionUnmarshaller
import org.phenoscape.kb.OWLFormats.OWLClassExpressionMarshaller
import org.phenoscape.kb.PhenexDataSet.DataSetMarshaller
import org.phenoscape.kb.Term.IRIsMarshaller
import org.phenoscape.kb.Term.JSONResultItemMarshaller
import org.phenoscape.kb.Term.JSONResultItemsMarshaller
import org.semanticweb.owlapi.apibinding.OWLManager
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLClass
import org.semanticweb.owlapi.model.OWLClassExpression
import org.semanticweb.owlapi.model.OWLNamedIndividual
import org.semanticweb.owlapi.vocab.OWLRDFVocabulary

import com.typesafe.config.ConfigFactory

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers
import akka.http.scaladsl.model.headers.`Cache-Control`
import akka.http.scaladsl.model.headers.CacheDirectives.`max-age`
import akka.http.scaladsl.model.headers.CacheDirectives.`must-revalidate`
import akka.http.scaladsl.model.headers.CacheDirectives.`s-maxage`
import akka.http.scaladsl.model.headers.ContentDispositionTypes
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.HttpApp
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.settings.ServerSettings
import akka.http.scaladsl.unmarshalling.Unmarshaller
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import scalaz._
import spray.json._
import spray.json.DefaultJsonProtocol._

object Main extends HttpApp with App {

  JenaSystem.init()

  implicit val system = ActorSystem("phenoscape-kb-system")
  import system.dispatcher

  val factory = OWLManager.getOWLDataFactory
  val owlClass = OWLRDFVocabulary.OWL_CLASS.getIRI
  val rdfsLabel = factory.getRDFSLabel.getIRI

  implicit val IRIUnmarshaller: Unmarshaller[String, IRI] = Unmarshaller.strict(IRI.create)

  implicit val IRISeqUnmarshaller: Unmarshaller[String, Seq[IRI]] = Unmarshaller.strict(_.split(",", -1).map(IRI.create))

  implicit val OWLClassUnmarshaller: Unmarshaller[String, OWLClass] = Unmarshaller.strict(text => factory.getOWLClass(IRI.create(text)))

  implicit val OWLNamedIndividualUnmarshaller: Unmarshaller[String, OWLNamedIndividual] = Unmarshaller.strict(text => factory.getOWLNamedIndividual(IRI.create(text)))

  implicit val SimpleMapFromJSONString: Unmarshaller[String, Map[String, String]] = Unmarshaller.strict { text =>
    text.parseJson match {
      case o: JsObject => o.fields.map { case (key, value) => key -> value.toString }
      case _           => throw new IllegalArgumentException(s"Not a valid JSON map: $text")
    }
  }

  implicit val SeqFromJSONString: Unmarshaller[String, Seq[String]] = Unmarshaller.strict { text =>
    text.parseJson match {
      case a: JsArray => a.elements.map(_.convertTo[String])
      case _          => throw new IllegalArgumentException(s"Not a valid JSON array: $text")
    }
  }

  val conf = ConfigFactory.load()
  val serverPort = conf.getInt("kb-services.port")
  val serverHost = conf.getString("kb-services.host")

  def routes: Route = cors() {
    respondWithHeaders(
      RawHeader("Vary", "negotiate, Accept"),
      `Cache-Control`(`must-revalidate`, `max-age`(0), `s-maxage`(2592001))) {
        pathSingleSlash {
          redirect(Uri("http://kb.phenoscape.org/apidocs/"), StatusCodes.SeeOther)
        } ~ pathPrefix("kb") {
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
                parameters('iri.as[IRI], 'definedBy.as[IRI].?) { (iri, source) =>
                  complete {
                    Term.classification(iri, source)
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
              pathPrefix("property_neighbors") {
                path("object") {
                  parameters('term.as[IRI], 'property.as[IRI]) { (term, property) =>
                    complete {
                      Graph.propertyNeighborsForObject(term, property)
                    }
                  }
                } ~
                  path("subject") {
                    parameters('term.as[IRI], 'property.as[IRI]) { (term, property) =>
                      complete {
                        Graph.propertyNeighborsForSubject(term, property)
                      }
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
              respondWithHeader(headers.`Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> "ontotrace.xml"))) {
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
            } ~ // why 2 graphs??
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
            path("search") { //undocumented and currently unused
              parameters('text, 'limit.as[Int]) { (text, limit) =>
                complete {
                  CharacterDescription.search(text, limit)
                }
              }
            } ~
              path("query") { //undocumented and currently unused
                parameters('entity.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'taxon.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (entity, taxon, limit, offset, total) =>
                  complete {
                    if (total) CharacterDescription.queryTotal(entity, taxon, Nil)
                    else CharacterDescription.query(entity, taxon, Nil, limit, offset)
                  }
                }
              } ~
              path("with_annotation") { //undocumented and currently unused
                parameter('iri.as[IRI]) { iri =>
                  complete {
                    CharacterDescription.annotatedCharacterDescriptionWithAnnotation(iri)
                  }
                }
              }
          } ~
          pathPrefix("taxon") {
            path("phenotypes") {
              parameters('taxon.as[IRI], 'entity.as[OWLClassExpression].?, 'quality.as[OWLClassExpression].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                (taxon, entityOpt, qualityOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset, total) =>
                  complete {
                    if (total) Taxon.directPhenotypesTotalFor(taxon, entityOpt, qualityOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                    else Taxon.directPhenotypesFor(taxon, entityOpt, qualityOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
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
                parameters('entity.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'quality.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                  (entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset, total) =>
                    complete {
                      if (total) {
                        Taxon.withPhenotypeTotal(entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                      } else Taxon.withPhenotype(entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                    }
                }
              } ~
              path("facet" / "phenotype" / Segment) { facetBy =>
                parameters('entity.as[IRI].?, 'quality.as[IRI].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false)) {
                  (entityOpt, qualityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs) =>
                    complete {
                      facetBy match {
                        case "entity"  => Taxon.facetTaxaWithPhenotypeByEntity(entityOpt, qualityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "quality" => Taxon.facetTaxaWithPhenotypeByQuality(qualityOpt, entityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "taxon"   => Taxon.facetTaxaWithPhenotypeByTaxon(taxonOpt, entityOpt, qualityOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                      }
                    }
                }
              } ~
              path("facet" / "annotations" / Segment) { facetBy =>
                parameters('entity.as[IRI].?, 'quality.as[IRI].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false)) {
                  (entityOpt, qualityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs) =>
                    complete {
                      facetBy match {
                        case "entity"  => TaxonPhenotypeAnnotation.facetTaxonAnnotationsByEntity(entityOpt, qualityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "quality" => TaxonPhenotypeAnnotation.facetTaxonAnnotationsByQuality(qualityOpt, entityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "taxon"   => TaxonPhenotypeAnnotation.facetTaxonAnnotationsByTaxon(taxonOpt, entityOpt, qualityOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                      }
                    }
                }
              } ~
              path("annotations") { //FIXME needs documentation
                parameters('entity.as[IRI].?, 'quality.as[IRI].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                  (entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset, total) =>
                    complete {
                      if (total) TaxonPhenotypeAnnotation.queryAnnotationsTotal(entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                      else TaxonPhenotypeAnnotation.queryAnnotations(entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                    }
                }
              } ~
              path("with_rank") {
                parameters('rank.as[IRI], 'in_taxon.as[IRI]) { (rank, inTaxon) =>
                  complete {
                    Taxon.taxaWithRank(rank, inTaxon)
                  }
                }
              } ~
              path("annotated_taxa_count") {
                parameter('in_taxon.as[IRI]) { inTaxon =>
                  complete {
                    Taxon.countOfAnnotatedTaxa(inTaxon).map(ResultCount(_))
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
              parameters('text, 'limit.as[Int].?(20)) { (text, limit) =>
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
              } ~
              path("homology") {
                parameters('entity.as[IRI]) { (entity) =>
                  complete {
                    AnatomicalEntity.homologyAnnotations(entity, false)
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
              path("phenotype_annotations") { // undocumented and not currently used
                parameters('entity.as[OWLClassExpression].?, 'quality.as[OWLClassExpression].?, 'in_taxon.as[IRI].?, 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                  (entity, quality, taxonOpt, limit, offset, total) =>
                    complete {
                      if (total) GenePhenotypeAnnotation.queryAnnotationsTotal(entity, quality, taxonOpt).map(ResultCount(_))
                      else GenePhenotypeAnnotation.queryAnnotations(entity, quality, taxonOpt, limit, offset)
                    }
                }
              } ~
              path("expression_annotations") { // undocumented and not currently used
                parameters('entity.as[OWLClassExpression].?, 'in_taxon.as[IRI].?, 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                  (entity, taxonOpt, limit, offset, total) =>
                    complete {
                      if (total) GeneExpressionAnnotation.queryAnnotationsTotal(entity, taxonOpt).map(ResultCount(_))
                      else GeneExpressionAnnotation.queryAnnotations(entity, taxonOpt, limit, offset)
                    }
                }
              } ~
              path("query") { // undocumented and not currently used
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
                parameters('iri.as[IRI], 'quality.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                  (iri, quality, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset, total) =>
                    complete {
                      if (total) Gene.affectingPhenotypeOfEntityTotal(iri, quality, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                      else Gene.affectingPhenotypeOfEntity(iri, quality, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
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
            path("query") { //FIXME doc out of date
              parameters('entity.as[IRI].?, 'quality.as[IRI].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                (entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset, total) =>
                  complete {
                    if (total) Study.queryStudiesTotal(entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                    else Study.queryStudies(entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                  }
              }
            } ~
              path("facet" / Segment) { facetBy =>
                parameters('entity.as[IRI].?, 'quality.as[IRI].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false)) {
                  (entityOpt, qualityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs) =>
                    complete {
                      facetBy match {
                        case "entity"  => Study.facetStudiesByEntity(entityOpt, qualityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "quality" => Study.facetStudiesByQuality(qualityOpt, entityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "taxon"   => Study.facetStudiesByTaxon(taxonOpt, entityOpt, qualityOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                      }
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
            path("query") {
              parameters('entity.as[IRI].?, 'quality.as[IRI].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                (entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset, total) =>
                  complete {
                    if (total) Phenotype.queryTaxonPhenotypesTotal(entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                    else Phenotype.queryTaxonPhenotypes(entity, quality, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                  }
              }
            } ~
              path("facet" / Segment) { facetBy =>
                parameters('entity.as[IRI].?, 'quality.as[IRI].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false)) {
                  (entityOpt, qualityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs) =>
                    complete {
                      facetBy match {
                        case "entity"  => Phenotype.facetPhenotypeByEntity(entityOpt, qualityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "quality" => Phenotype.facetPhenotypeByQuality(qualityOpt, entityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "taxon"   => Phenotype.facetPhenotypeByTaxon(taxonOpt, entityOpt, qualityOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                      }
                    }
                }
              } ~
              path("direct_annotations") { // undocumented and not currently used
                parameters('iri.as[IRI]) { (iri) =>
                  complete {
                    CharacterDescription.eqAnnotationsForPhenotype(iri)
                  }
                }
              } ~
              path("nearest_eq") { // undocumented and not currently used
                parameters('iri.as[IRI]) { (iri) =>
                  complete {
                    Phenotype.eqForPhenotype(iri)
                  }
                }
              }
          } ~
          pathPrefix("report") {
            path("data_coverage_figure") { // undocumented and not currently used
              complete {
                DataCoverageFigureReport.query()
              }
            } ~
              path("data_coverage_figure_catfish") { // undocumented and not currently used
                complete {
                  DataCoverageFigureReportCatfish.query()
                }
              } ~
              path("data_coverage_figure_any_taxon") { // undocumented and not currently used
                complete {
                  DataCoverageFigureReportAnyTaxon.query()
                }
              }
          }
        //          ~
        //          path("test") {
        //            complete {
        //              Facets.facetTaxaWithPhenotype(IRI.create("http://purl.obolibrary.org/obo/UBERON_0010740")).map(_.toString)
        //            }
        //          }
      }
  }

  val log = Logging(system, this.getClass)

  startServer(host = serverHost, port = serverPort, settings = ServerSettings(conf), system = system)

}