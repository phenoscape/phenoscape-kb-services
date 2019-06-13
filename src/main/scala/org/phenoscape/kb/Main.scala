package org.phenoscape.kb

import org.apache.jena.system.JenaSystem
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.OWLFormats.ManchesterSyntaxClassExpressionUnmarshaller
import org.phenoscape.kb.OWLFormats.OWLClassExpressionMarshaller
import org.phenoscape.kb.PhenexDataSet.DataSetMarshaller
import org.phenoscape.kb.Term.IRIsMarshaller
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
import akka.http.scaladsl.model.HttpHeader
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.Uri
import akka.http.scaladsl.model.headers
import akka.http.scaladsl.model.headers.ContentDispositionTypes
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.HttpApp
import akka.http.scaladsl.server.RequestContext
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.settings.ServerSettings
import akka.http.scaladsl.unmarshalling.Unmarshaller
import akka.http.scaladsl.server.directives.CachingDirectives._
import ch.megard.akka.http.cors.scaladsl.CorsDirectives._
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import org.phenoscape.kb.queries.QueryUtil.{InferredAbsence, InferredPresence, PhenotypicQuality, QualitySpec}
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

  implicit val QualitySpecUnmarshaller: Unmarshaller[String, QualitySpec] = IRIUnmarshaller.map(QualitySpec.fromIRI)

  implicit val IRISeqUnmarshaller: Unmarshaller[String, Seq[IRI]] = Unmarshaller.strict(_.split(",", -1).map(IRI.create)) //FIXME standardize services to use the JSON array unmarshaller, currently Seq[String]

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

  val cacheKeyer: PartialFunction[RequestContext, (Uri, Option[HttpHeader])] = {
    case r: RequestContext if (r.request.method == GET) => (r.request.uri, r.request.headers.find(_.is("accept")))
  }
  val memoryCache = routeCache[(Uri, Option[HttpHeader])]

  val conf = ConfigFactory.load()
  val serverPort = conf.getInt("kb-services.port")
  val serverHost = conf.getString("kb-services.host")

  val corsSettings = CorsSettings.defaultSettings.withAllowCredentials(false)

  def routes: Route = cors() {
    alwaysCache(memoryCache, cacheKeyer) {
      respondWithHeaders(RawHeader("Vary", "negotiate, Accept")) {
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
              parameters('text, 'type.as[IRI].?(owlClass), 'properties.as[Seq[String]].?, 'definedBy.as[Seq[String]].?, 'includeDeprecated.as[Boolean].?(false), 'limit.as[Int].?(100)) { (text, termType, properties, definedByOpt, includeDeprecated, limit) =>
                complete {
                  val props = properties.map(_.map(IRI.create)).getOrElse(List(rdfsLabel, hasExactSynonym.getIRI, hasNarrowSynonym.getIRI, hasBroadSynonym.getIRI))
                  val definedBys = definedByOpt.map(_.map(IRI.create)).getOrElse(Nil)
                  import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                  Term.search(text, termType, props, definedBys, includeDeprecated, limit)
                }
              }
            } ~
              path("search_classes") {
                parameters('text, 'definedBy.as[IRI], 'limit.as[Int].?(0)) { (text, definedBy, limit) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
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
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
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
                parameters('iri.as[IRI], 'parts.as[Boolean].?(false)) { (iri, includeAsPart) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    Term.allAncestors(iri, includeAsPart)
                  }
                }
              } ~
              path("all_descendants") {
                parameters('iri.as[IRI], 'parts.as[Boolean].?(false)) { (iri, includeParts) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    Term.allDescendants(iri, includeParts)
                  }
                }
              } ~
              pathPrefix("property_neighbors") {
                path("object") {
                  parameters('term.as[IRI], 'property.as[IRI]) { (term, property) =>
                    complete {
                      import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                      Graph.propertyNeighborsForObject(term, property)
                    }
                  }
                } ~
                  path("subject") {
                    parameters('term.as[IRI], 'property.as[IRI]) { (term, property) =>
                      complete {
                        import org.phenoscape.kb.Term.JSONResultItemsMarshaller
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
            get {
              parameters('entity.as[OWLClassExpression], 'taxon.as[OWLClassExpression], 'variable_only.as[Boolean].?(true), 'parts.as[Boolean].?(false)) { (entity, taxon, variableOnly, includeParts) =>
                respondWithHeader(headers.`Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> "ontotrace.xml"))) {
                  complete {
                    PresenceAbsenceOfStructure.presenceAbsenceMatrix(entity, taxon, variableOnly, includeParts)
                  }
                }
              }
            } ~
              post {
                formFields('entity.as[OWLClassExpression], 'taxon.as[OWLClassExpression], 'variable_only.as[Boolean].?(true), 'parts.as[Boolean].?(false)) { (entity, taxon, variableOnly, includeParts) =>
                  respondWithHeader(headers.`Content-Disposition`(ContentDispositionTypes.attachment, Map("filename" -> "ontotrace.xml"))) {
                    complete {
                      PresenceAbsenceOfStructure.presenceAbsenceMatrix(entity, taxon, variableOnly, includeParts)
                    }
                  }
                }
              }
          } ~
          pathPrefix("similarity") {
            path("query") {
              parameters('iri.as[IRI], 'corpus_graph.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0)) { (query, corpusGraph, limit, offset) =>
                complete {
                  import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                  Similarity.querySimilarProfiles(query, corpusGraph, limit, offset)
                }
              }
            } ~ // why 2 graphs??
              path("best_matches") {
                parameters('query_iri.as[IRI], 'corpus_iri.as[IRI], 'query_graph.as[IRI], 'corpus_graph.as[IRI]) { (queryItem, corpusItem, queryGraph, corpusGraph) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    Similarity.bestAnnotationsMatchesForComparison(queryItem, queryGraph, corpusItem, corpusGraph)
                  }
                }
              } ~
              path("best_subsumers") {
                parameters('query_iri.as[IRI], 'corpus_iri.as[IRI], 'corpus_graph.as[IRI]) { (queryItem, corpusItem, corpusGraph) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    Similarity.bestSubsumersForComparison(queryItem, corpusItem, corpusGraph)
                  }
                }
              } ~
              path("subsumed_annotations") {
                parameters('subsumer.as[OWLClass], 'instance.as[OWLNamedIndividual]) { (subsumer, instance) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
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
                parameters('iri.as[OWLClass], 'query_graph.as[IRI], 'corpus_graph.as[IRI]) { (term, queryGraph, corpusGraph) =>
                  complete {
                    Similarity.icDisparity(term, queryGraph, corpusGraph).map(value => JsObject("value" -> value.toJson))
                  }
                }
              } ~
              path("states") {
                parameters('leftStudy.as[IRI], 'leftCharacter.as[Int], 'leftSymbol, 'rightStudy.as[IRI], 'rightCharacter.as[Int], 'rightSymbol) { (leftStudyIRI, leftCharacterNum, leftSymbol, rightStudyIRI, rightCharacterNum, rightSymbol) =>
                  complete {
                    Similarity.stateSimilarity(leftStudyIRI, leftCharacterNum, leftSymbol, rightStudyIRI, rightCharacterNum, rightSymbol).map(_.toJson)
                  }
                }
              } ~
              path("jaccard") { //FIXME can GET and POST share code better?
                get {
                  parameters('iris.as[Seq[String]]) { iriStrings =>
                    complete {
                      import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                      val iris = iriStrings.map(IRI.create)
                      Similarity.pairwiseJaccardSimilarity(iris.toSet)
                    }
                  }
                } ~
                  post {
                    formFields('iris.as[Seq[String]]) { iriStrings =>
                      complete {
                        import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                        val iris = iriStrings.map(IRI.create)
                        Similarity.pairwiseJaccardSimilarity(iris.toSet)
                      }
                    }
                  }
              } ~
              path("matrix") {
                get {
                  parameters('terms.as[Seq[String]]) { iriStrings =>
                    complete {
                      val iris = iriStrings.map(IRI.create).toSet
                      Graph.ancestorMatrix(iris)
                    }
                  }
                } ~
                  post {
                    formFields('terms.as[Seq[String]]) { iriStrings =>
                      complete {
                        val iris = iriStrings.map(IRI.create).toSet
                        Graph.ancestorMatrix(iris)
                      }
                    }
                  }
              }
          } ~
          pathPrefix("characterstate") {
            path("search") { //undocumented and currently unused
              parameters('text, 'limit.as[Int]) { (text, limit) =>
                complete {
                  import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                  CharacterDescription.search(text, limit)
                }
              }
            } ~
              path("query") { //undocumented and currently unused
                parameters('entity.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'taxon.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (entity, taxon, limit, offset, total) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
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
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    val entityIsNamed = entityOpt.forall(!_.isAnonymous)
                    val qualityIsNamed = qualityOpt.forall(!_.isAnonymous)
                    if (entityIsNamed && qualityIsNamed) {
                      val entityIRI = entityOpt.map(_.asOWLClass).filterNot(_.isOWLThing).map(_.getIRI)
                      val qualitySpec = qualityOpt.map(_.asOWLClass).filterNot(_.isOWLThing).map(_.getIRI).map(QualitySpec.fromIRI).getOrElse(PhenotypicQuality(None))
                      if (total) Taxon.directPhenotypesTotalFor(taxon, entityIRI, qualitySpec, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                      else Taxon.directPhenotypesFor(taxon, entityIRI, qualitySpec, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                    } else {
                      if (total) Taxon.directPhenotypesTotalForExpression(taxon, entityOpt, qualityOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                      else Taxon.directPhenotypesForExpression(taxon, entityOpt, qualityOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                    }
                  }
              }
            } ~
              path("variation_profile") {
                parameters('taxon.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (taxon, limit, offset, total) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    if (total) Taxon.variationProfileTotalFor(taxon).map(ResultCount(_))
                    else Taxon.variationProfileFor(taxon, limit, offset)
                  }
                }
              } ~
              path("with_phenotype") {
                parameters('entity.as[OWLClassExpression].?, 'quality.as[OWLClassExpression].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                  (entityOpt, qualityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset, total) =>
                    complete {
                      import org.phenoscape.kb.Taxon.ComboTaxaMarshaller
                      val entityIsNamed = entityOpt.forall(!_.isAnonymous)
                      val qualityIsNamed = qualityOpt.forall(!_.isAnonymous)
                      if (entityIsNamed && qualityIsNamed) {
                        val entityIRI = entityOpt.map(_.asOWLClass).filterNot(_.isOWLThing).map(_.getIRI)
                        val qualitySpec = qualityOpt.map(_.asOWLClass).filterNot(_.isOWLThing).map(_.getIRI).map(QualitySpec.fromIRI).getOrElse(PhenotypicQuality(None))
                        if (total) Taxon.withPhenotypeTotal(entityIRI, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                        else Taxon.withPhenotype(entityIRI, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                      } else {
                        if (total) Taxon.withPhenotypeExpressionTotal(entityOpt.getOrElse(owlThing), qualityOpt.getOrElse(owlThing), taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                        else Taxon.withPhenotypeExpression(entityOpt.getOrElse(owlThing), qualityOpt.getOrElse(owlThing), taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                      }
                    }
                }
              } ~
              path("facet" / "phenotype" / Segment) { facetBy =>
                parameters('entity.as[IRI].?, 'quality.as[QualitySpec].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false)) {
                  (entityOpt, qualitySpecOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs) =>
                    complete {
                      val qualitySpec = qualitySpecOpt.getOrElse(PhenotypicQuality(None))
                      facetBy match {
                        case "entity"  => Taxon.facetTaxaWithPhenotypeByEntity(entityOpt, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "quality" => Taxon.facetTaxaWithPhenotypeByQuality(qualitySpec.asOptionalQuality, entityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "taxon"   => Taxon.facetTaxaWithPhenotypeByTaxon(taxonOpt, entityOpt, qualitySpec, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                      }
                    }
                }
              } ~
              path("facet" / "annotations" / Segment) { facetBy =>
                parameters('entity.as[IRI].?, 'quality.as[QualitySpec].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false)) {
                  (entityOpt, qualitySpecOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs) =>
                    complete {
                      val qualitySpec = qualitySpecOpt.getOrElse(PhenotypicQuality(None))
                      facetBy match {

                        case "entity"  => TaxonPhenotypeAnnotation.facetTaxonAnnotationsByEntity(entityOpt, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "quality" => TaxonPhenotypeAnnotation.facetTaxonAnnotationsByQuality(qualitySpec.asOptionalQuality, entityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "taxon"   => TaxonPhenotypeAnnotation.facetTaxonAnnotationsByTaxon(taxonOpt, entityOpt, qualitySpec, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                      }
                    }
                }
              } ~
              path("annotations") { //FIXME needs documentation
                parameters('entity.as[IRI].?, 'quality.as[QualitySpec].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                  (entity, qualitySpecOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset, total) =>
                    complete {
                      val qualitySpec = qualitySpecOpt.getOrElse(PhenotypicQuality(None))
                      if (total) TaxonPhenotypeAnnotation.queryAnnotationsTotal(entity, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                      else TaxonPhenotypeAnnotation.queryAnnotations(entity, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                    }
                }
              } ~
              path("annotation" / "sources") { //FIXME needs documentation
                parameters('taxon.as[IRI], 'phenotype.as[IRI]) {
                  (taxon, phenotype) =>
                    complete {
                      import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                      TaxonPhenotypeAnnotation.annotationSources(taxon, phenotype)
                    }
                }
              } ~
              path("with_rank") {
                parameters('rank.as[IRI], 'in_taxon.as[IRI]) { (rank, inTaxon) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
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
                  import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                  Term.searchOntologyTerms(text, Uberon, limit)
                }
              }
            } ~
              pathPrefix("absence") {
                path("evidence") {
                  parameters('taxon.as[IRI], 'entity.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (taxon, entity, limit, offset, totalOnly) =>
                    complete {
                      import org.phenoscape.kb.Term.JSONResultItemsMarshaller
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
                      import org.phenoscape.kb.Term.JSONResultItemsMarshaller
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
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    AnatomicalEntity.homologyAnnotations(entity, false)
                  }
                }
              } ~
              path("dependency") {
                get {
                  parameters('terms.as[Seq[String]]) { iriStrings =>
                    complete {
                      val iris = iriStrings.map(IRI.create).toSet
                      AnatomicalEntity.presenceAbsenceDependencyMatrix(iris)
                    }
                  }
                }
              }
          } ~
          pathPrefix("gene") {
            path("search") {
              parameters('text, 'taxon.as[IRI].?) { (text, taxonOpt) => //FIXME add limit option?
                complete {
                  import org.phenoscape.kb.Term.JSONResultItemsMarshaller
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
                      import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                      if (total) GeneExpressionAnnotation.queryAnnotationsTotal(entity, taxonOpt).map(ResultCount(_))
                      else GeneExpressionAnnotation.queryAnnotations(entity, taxonOpt, limit, offset)
                    }
                }
              } ~
              path("query") { // undocumented and not currently used
                parameters('entity.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'taxon.as[OWLClassExpression].?(owlThing: OWLClassExpression), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (entity, taxon, limit, offset, total) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    if (total) Gene.queryTotal(entity, taxon)
                    else Gene.query(entity, taxon, limit, offset)
                  }
                }
              } ~
              path("phenotypic_profile") {
                parameters('iri.as[IRI]) { (iri) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    Gene.phenotypicProfile(iri)
                  }
                }
              } ~
              path("expression_profile") {
                parameters('iri.as[IRI]) { (iri) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    Gene.expressionProfile(iri)
                  }
                }
              } ~
              path("affecting_entity_phenotype") {
                //TODO update documentation that iri is optional
                parameters('iri.as[IRI].?, 'quality.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                  (iri, quality, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset, total) =>
                    complete {
                      import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                      if (total) Gene.affectingPhenotypeOfEntityTotal(iri, quality, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                      else Gene.affectingPhenotypeOfEntity(iri, quality, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                    }
                }
              } ~
              path("expressed_within_entity") {
                parameters('iri.as[IRI], 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) { (iri, limit, offset, total) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    if (total) Gene.expressedWithinEntityTotal(iri).map(ResultCount(_))
                    else Gene.expressedWithinEntity(iri, limit, offset)
                  }
                }
              } ~
              path("facet" / "phenotype" / Segment) { facetBy =>
                parameters('entity.as[IRI].?, 'quality.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false)) {
                  (entityOpt, qualityOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs) =>
                    complete {
                      facetBy match {
                        case "entity"  => Gene.facetGenesWithPhenotypeByEntity(entityOpt, qualityOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "quality" => Gene.facetGenesWithPhenotypeByQuality(qualityOpt, entityOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                      }
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
              parameters('entity.as[IRI].?, 'quality.as[QualitySpec].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                (entity, qualitySpecOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset, total) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    val qualitySpec = qualitySpecOpt.getOrElse(PhenotypicQuality(None))
                    if (total) Study.queryStudiesTotal(entity, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                    else Study.queryStudies(entity, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                  }
              }
            } ~
              path("facet" / Segment) { facetBy =>
                parameters('entity.as[IRI].?, 'quality.as[QualitySpec].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false)) {
                  (entityOpt, qualitySpecOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs) =>
                    complete {
                      val qualitySpec = qualitySpecOpt.getOrElse(PhenotypicQuality(None))
                      facetBy match {

                        case "entity"  => Study.facetStudiesByEntity(entityOpt, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "quality" => Study.facetStudiesByQuality(qualitySpec.asOptionalQuality, entityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "taxon"   => Study.facetStudiesByTaxon(taxonOpt, entityOpt, qualitySpec, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
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
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    if (total) Study.annotatedPhenotypesTotal(studyIRI).map(ResultCount(_))
                    else Study.annotatedPhenotypes(studyIRI, limit, offset)
                  }
                }
              } ~
              path("matrix") {
                parameters('iri.as[IRI]) { iri =>
                  complete {
                    Study.queryMatrix(iri)
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
              parameters('entity.as[IRI].?, 'quality.as[QualitySpec].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false), 'limit.as[Int].?(20), 'offset.as[Int].?(0), 'total.as[Boolean].?(false)) {
                (entity, qualitySpecOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset, total) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
                    val qualitySpec = qualitySpecOpt.getOrElse(PhenotypicQuality(None))
                    if (total) Phenotype.queryTaxonPhenotypesTotal(entity, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs).map(ResultCount(_))
                    else Phenotype.queryTaxonPhenotypes(entity, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs, limit, offset)
                  }
              }
            } ~
              path("facet" / Segment) { facetBy =>
                parameters('entity.as[IRI].?, 'quality.as[QualitySpec].?, 'in_taxon.as[IRI].?, 'publication.as[IRI].?, 'parts.as[Boolean].?(false), 'historical_homologs.as[Boolean].?(false), 'serial_homologs.as[Boolean].?(false)) {
                  (entityOpt, qualitySpecOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs) =>
                    complete {
                      val qualitySpec = qualitySpecOpt.getOrElse(PhenotypicQuality(None))
                      facetBy match {

                        case "entity"  => Phenotype.facetPhenotypeByEntity(entityOpt, qualitySpec, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "quality" => Phenotype.facetPhenotypeByQuality(qualitySpec.asOptionalQuality, entityOpt, taxonOpt, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                        case "taxon"   => Phenotype.facetPhenotypeByTaxon(taxonOpt, entityOpt, qualitySpec, pubOpt, includeParts, includeHistoricalHomologs, includeSerialHomologs)
                      }
                    }
                }
              } ~
              path("direct_annotations") { // undocumented and not currently used //FIXME actually this is used in a popup in web UI
                parameters('iri.as[IRI]) { (iri) =>
                  complete {
                    import org.phenoscape.kb.Term.JSONResultItemsMarshaller
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
      }
    }
  }

  val log = Logging(system, this.getClass)

  startServer(host = serverHost, port = serverPort, settings = ServerSettings(conf), system = system)

}