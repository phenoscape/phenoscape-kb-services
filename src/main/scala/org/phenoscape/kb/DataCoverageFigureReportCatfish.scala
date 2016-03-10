package org.phenoscape.kb

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLClassExpression
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVarDistinct
import com.hp.hpl.jena.query.QuerySolution
import scala.language.postfixOps

object DataCoverageFigureReportCatfish {

  val entities = Set(
    "pectoral fin" -> "http://purl.obolibrary.org/obo/UBERON_0000151",
    "pectoral girdle skeleton" -> "http://purl.obolibrary.org/obo/UBERON_0007831",
    "pelvic fin" -> "http://purl.obolibrary.org/obo/UBERON_0000152",
    "pelvic girdle skeleton" -> "http://purl.obolibrary.org/obo/UBERON_0007832",
    "dorsal fin" -> "http://purl.obolibrary.org/obo/UBERON_0003097",
    "anal fin" -> "http://purl.obolibrary.org/obo/UBERON_4000163",
    "caudal fin" -> "http://purl.obolibrary.org/obo/UBERON_4000164",
    "adipose fin" -> "http://purl.obolibrary.org/obo/UBERON_2000251",
    "hyoid arch skeleton" -> "http://purl.obolibrary.org/obo/UBERON_0005884",
    "jaw region" -> "http://purl.obolibrary.org/obo/UBERON_0011595",
    "post-hyoid pharyngeal arch skeleton" -> "http://purl.obolibrary.org/obo/UBERON_0005886",
    "neurocranium" -> "http://purl.obolibrary.org/obo/UBERON_0001703",
    "post-cranial axial skeletal system" -> "http://purl.obolibrary.org/obo/UBERON_0011138",
    "integumental system" -> "http://purl.obolibrary.org/obo/UBERON_0002416")

  val Siluriformes = Class("http://purl.obolibrary.org/obo/VTO_0034991")

  def query(): Future[String] = {
    val results = for {
      (entityLabel, entityIRI) <- entities
    } yield {
      queryEntry(entityIRI, entityLabel)
    }
    Future.sequence(results).map { groups =>
      val entries = for {
        group <- groups
        queryResult <- group
      } yield {
        queryResult.toString
      }
      entries.mkString("\n")
    }
  }

  private def processResult(entity: IRI, entityLabel: String, solution: QuerySolution): QueryResult = {
    val taxon = IRI.create(solution.getResource("taxon").getURI)
    val taxonLabel = solution.getLiteral("taxon_label").getLexicalForm
    val count = solution.getLiteral("count").getInt
    QueryResult(entity, entityLabel, taxon, taxonLabel, count)
  }

  private def queryEntry(entityIRI: String, entityLabel: String): Future[Seq[QueryResult]] = {
    val query = buildQuery(entityIRI)
    for {
      expandedQuery <- App.expandWithOwlet(query)
      _ = println(s"Expanded $entityLabel")
      result <- App.executeSPARQLQuery(expandedQuery, processResult(IRI.create(entityIRI), entityLabel, _))
      _ = println(s"Queried $entityLabel")
    } yield {
      result
    }
  }

  private def buildQuery(entityIRI: String): Query = {
    val entityClass = Class(IRI.create(entityIRI))
    val entityInd = Individual(entityIRI)
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, exhibits_state, 'state),
        t('taxon, rdfsLabel, 'taxon_label),
        t('state, describes_phenotype, 'phenotype),
        t('taxon, ObjectProperty(rdfsSubClassOf) *, Siluriformes),
        t('phenotype, rdfsSubClassOf, ((phenotype_of some (part_of some entityClass)) or (towards value entityInd)).asOMN)))
    query.getProject.add(Var.alloc("taxon"))
    query.getProject.add(Var.alloc("taxon_label"))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("state"))))
    query.addGroupBy("taxon")
    query.addGroupBy("taxon_label")
    query
  }

  case class QueryResult(entity: IRI, entityLabel: String, taxon: IRI, taxonLabel: String, count: Int) {

    override def toString = s"$entityLabel\t$taxonLabel\t$count"

  }

}