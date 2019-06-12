package org.phenoscape.kb

import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.MediaTypes
import org.apache.jena.query.{QueryFactory, QuerySolution}
import org.phenoscape.kb.KBVocab.{rdfsSubClassOf, _}
import org.phenoscape.kb.Similarity.rdfsSubClassOf
import org.phenoscape.owl.Vocab._
import org.phenoscape.scowl._
import org.phenoscape.sparql.SPARQLInterpolation._
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.owl.{NamedRestrictionGenerator, Vocab}
import org.semanticweb.owlapi.model.IRI
import spray.json.DefaultJsonProtocol._
import spray.json._
import org.phenoscape.kb.Main.system.dispatcher

import scala.concurrent.Future

object AnatomicalEntity {

  private val dcSource = ObjectProperty(IRI.create("http://purl.org/dc/elements/1.1/source"))
  private val ECO = IRI.create("http://purl.obolibrary.org/obo/eco.owl")
  private val implies_presence_of_some = NamedRestrictionGenerator.getClassRelationIRI(Vocab.IMPLIES_PRESENCE_OF.getIRI)

  def homologyAnnotations(term: IRI, includeSubClasses: Boolean): Future[Seq[HomologyAnnotation]] = App.executeSPARQLQueryString(homologyAnnotationQuery(term, includeSubClasses), HomologyAnnotation(_, term))

  private def homologyAnnotationQuery(term: IRI, includeSubClasses: Boolean): String = {
    val termSpec = if (includeSubClasses) sparql"GRAPH $KBClosureGraph { ?term $rdfsSubClassOf $term . } "
    else sparql"VALUES ?term { $term }"
    val query =
      sparql"""
      SELECT DISTINCT ?subject ?object ?subjectTaxon ?subjectVTO ?objectTaxon ?objectVTO ?negated ?source ?evidenceType ?relation
      FROM $KBMainGraph
      WHERE {
        VALUES ?relation { $homologous_to $serially_homologous_to }
        $termSpec
        ?annotation ?associationHasPredicate ?relation .
        ?annotation $associationHasSubject/$rdfType/$owlIntersectionOf/($rdfRest*)/$rdfFirst ?subject .
        ?annotation $associationHasSubject/$rdfType/$owlIntersectionOf/($rdfRest*)/$rdfFirst ?subjectTaxonRestriction .
        ?subjectTaxonRestriction $owlOnProperty $in_taxon .
        ?subjectTaxonRestriction $owlSomeValuesFrom ?subjectTaxon .
        ?annotation $associationHasObject/$rdfType/$owlIntersectionOf/($rdfRest*)/$rdfFirst ?object .
        ?annotation $associationHasObject/$rdfType/$owlIntersectionOf/($rdfRest*)/$rdfFirst ?objectTaxonRestriction .
        ?objectTaxonRestriction $owlOnProperty $in_taxon .
        ?objectTaxonRestriction $owlSomeValuesFrom ?objectTaxon .
        ?annotation $associationIsNegated ?negated .
        ?annotation $has_evidence ?evidence .
        ?evidence $dcSource ?source .
        ?evidence $rdfType ?evidenceType .
        ?evidenceType $rdfsIsDefinedBy $ECO .
        FILTER(isIRI(?subject))
        FILTER(isIRI(?object))
        FILTER((?subject = ?term) || (?object = ?term))
        OPTIONAL {
          ?subjectTaxon $owlEquivalentClass ?subjectVTO .
          ?subjectVTO $rdfsIsDefinedBy $VTO .
          FILTER(isIRI(?subjectVTO))
        }
        OPTIONAL {
          ?objectTaxon $owlEquivalentClass ?objectVTO .
          ?objectVTO $rdfsIsDefinedBy $VTO .
          FILTER(isIRI(?objectVTO))
        }
      }
      """
    query.text
  }

  // Output a boolean matrix as CSV
  def matrixRendererFromMapOfMaps[A](dependencyMatrix: DependencyMatrix[A]) = {

    val mapOfMaps = dependencyMatrix.map
    val sortedKeys = mapOfMaps.keys.toList.sortBy(_.toString)
    val headers = s",${sortedKeys.mkString(",")}" //print column headers

    val matrix = for (x <- sortedKeys) yield {
      val row = s"$x"
      val values = for (y <- sortedKeys) yield mapOfMaps(x)(y) match {
        case true => 1
        case false => 0
      }
      s"$row, ${values.mkString(",")}"
    }
    s"$headers\n${matrix.mkString("\n")}"
  }

  def presenceAbsenceDependencyMatrix(iris: Set[IRI]): Future[DependencyMatrix[IRI]] = {
    import org.phenoscape.kb.util.Util.TraversableOps
    import org.phenoscape.kb.util.Util.MapOps
    val dependencyTuples = for {
      x <- iris
      y <- iris
    } yield if (x == y) Future.successful(x -> (y -> true)) else presenceImpliesPresenceOf(x, y).map(e => x -> (y -> e))

    //Convert from Set(x, (y, flag)) -> Map[x -> Map[y -> flag]]
    Future.sequence(dependencyTuples).map { deps =>
      DependencyMatrix(deps.groupMap(_._1)(_._2).mapVals(_.toMap))
    }
  }


  def presenceImpliesPresenceOf(x: IRI, y: IRI): Future[Boolean] = {
    App.executeSPARQLAskQuery(QueryFactory.create(queryImpliesPresenceOf(x, y).text))
  }

  private def queryImpliesPresenceOf(x: IRI, y: IRI): QueryText =
    sparql"""
            ASK
            FROM $KBClosureGraph
            FROM $KBMainGraph
            WHERE {
              ?x_presence $implies_presence_of_some $x .
              ?y_presence $implies_presence_of_some $y .
              ?x_presence $rdfsSubClassOf ?y_presence
            }
        """
}

final case class HomologyAnnotation(subject: IRI, subjectTaxon: IRI, `object`: IRI, objectTaxon: IRI, source: String, evidence: IRI, negated: Boolean, relation: IRI) extends JSONResultItem {

  def toJSON: JsObject = Map(
    "subject" -> subject.toString.toJson,
    "subjectTaxon" -> subjectTaxon.toString.toJson,
    "object" -> `object`.toString.toJson,
    "objectTaxon" -> objectTaxon.toString.toJson,
    "source" -> source.toJson,
    "negated" -> negated.toJson,
    "evidence" -> evidence.toString.toJson,
    "relation" -> relation.toString.toJson)
    .toJson.asJsObject

}

object HomologyAnnotation {

  def apply(querySolution: QuerySolution, queriedTerm: IRI): HomologyAnnotation = {
    val querySubject = IRI.create(querySolution.getResource("subject").getURI)
    val queryObject = IRI.create(querySolution.getResource("object").getURI)
    val querySubjectTaxon = {
      val st = querySolution.getResource("subjectTaxon").getURI
      if ((!st.startsWith("http://purl.obolibrary.org/obo/VTO_")) && querySolution.contains("subjectVTO"))
        IRI.create(querySolution.getResource("subjectVTO").getURI)
      else IRI.create(st)
    }
    val queryObjectTaxon = {
      val st = querySolution.getResource("objectTaxon").getURI
      if ((!st.startsWith("http://purl.obolibrary.org/obo/VTO_")) && querySolution.contains("objectVTO"))
        IRI.create(querySolution.getResource("objectVTO").getURI)
      else IRI.create(st)
    }
    val (annotationSubject, annotationSubjectTaxon, annotationObject, annotationObjectTaxon) = if (querySubject == queriedTerm) (querySubject, querySubjectTaxon, queryObject, queryObjectTaxon)
    else (queryObject, queryObjectTaxon, querySubject, querySubjectTaxon)
    HomologyAnnotation(
      annotationSubject,
      annotationSubjectTaxon,
      annotationObject,
      annotationObjectTaxon,
      querySolution.getLiteral("source").getLexicalForm,
      IRI.create(querySolution.getResource("evidenceType").getURI),
      querySolution.getLiteral("negated").getBoolean,
      IRI.create(querySolution.getResource("relation").getURI))
  }

}

final case class DependencyMatrix[A](map: Map[A, Map[A, Boolean]])

object DependencyMatrix {

  implicit val csvMarshaller : ToEntityMarshaller[DependencyMatrix[_]] = Marshaller.stringMarshaller(MediaTypes.`text/plain`).compose(matrix =>
    AnatomicalEntity.matrixRendererFromMapOfMaps(matrix))

}
