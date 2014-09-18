package org.phenoscape.kb

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl.OWL._
import org.semanticweb.owlapi.model.IRI

import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVarDistinct

object DataCoverageFigureReportAnyTaxon {

  val entities = Set(
    "anocleithrum" -> "http://purl.obolibrary.org/obo/UBERON_4000160",
    "basipterygium bone" -> "http://purl.obolibrary.org/obo/UBERON_2000623",
    "carpal bone" -> "http://purl.obolibrary.org/obo/UBERON_0001435",
    "clavicle" -> "http://purl.obolibrary.org/obo/UBERON_0001105",
    "cleithrum" -> "http://purl.obolibrary.org/obo/UBERON_0004741",
    "coracoid bone" -> "http://purl.obolibrary.org/obo/UBERON_0004743",
    "digit" -> "http://purl.obolibrary.org/obo/UBERON_0002544",
    "epicoracoid" -> "http://purl.obolibrary.org/obo/UBERON_3000762",
    "epipubis" -> "http://purl.obolibrary.org/obo/UBERON_3000884",
    "extrascapula" -> "http://purl.obolibrary.org/obo/UBERON_2000663",
    "extracleithrum" -> "http://purl.obolibrary.org/obo/UBERON_4200022",
    "femur" -> "http://purl.obolibrary.org/obo/UBERON_0000981",
    "fibula" -> "http://purl.obolibrary.org/obo/UBERON_0001446",
    "humerus" -> "http://purl.obolibrary.org/obo/UBERON_0000976",
    "ilium" -> "http://purl.obolibrary.org/obo/UBERON_0001273",
    "interclavicle" -> "http://purl.obolibrary.org/obo/UBERON_0011655",
    "ischium" -> "http://purl.obolibrary.org/obo/UBERON_0001274",
    "manual digit" -> "http://purl.obolibrary.org/obo/UBERON_0002389",
    "metacarpal bone" -> "http://purl.obolibrary.org/obo/UBERON_0002374",
    "metatarsal bone" -> "http://purl.obolibrary.org/obo/UBERON_0001448",
    "paired fin radial bone" -> "http://purl.obolibrary.org/obo/UBERON_1500006",
    "pectoral girdle bone" -> "http://purl.obolibrary.org/obo/UBERON_0007829",
    "pectoral fin radial bone" -> "http://purl.obolibrary.org/obo/UBERON_2001586",
    "pectoral fin lepidotrichium" -> "http://purl.obolibrary.org/obo/UBERON_4000175",
    "pectoral girdle skeleton" -> "http://purl.obolibrary.org/obo/UBERON_0007831",
    "pedal digit" -> "http://purl.obolibrary.org/obo/UBERON_0001466",
    "pelvic fin radial bone" -> "http://purl.obolibrary.org/obo/UBERON_2000508",
    "pelvic fin lepidotrichium" -> "http://purl.obolibrary.org/obo/UBERON_4000173",
    "pelvic girdle skeleton" -> "http://purl.obolibrary.org/obo/UBERON_0007832",
    "pelvic girdle bone/zone" -> "http://purl.obolibrary.org/obo/UBERON_0007830",
    "phalanx of manus" -> "http://purl.obolibrary.org/obo/UBERON_0001436",
    "phalanx of pes" -> "http://purl.obolibrary.org/obo/UBERON_0001449",
    "postcleithrum" -> "http://purl.obolibrary.org/obo/UBERON_2000410",
    "pubis" -> "http://purl.obolibrary.org/obo/UBERON_0001275",
    "radius bone" -> "http://purl.obolibrary.org/obo/UBERON_0001423",
    "scapula" -> "http://purl.obolibrary.org/obo/UBERON_0006849",
    "sternum" -> "http://purl.obolibrary.org/obo/UBERON_0000975",
    "tarsal bone" -> "http://purl.obolibrary.org/obo/UBERON_0001447",
    "tibia" -> "http://purl.obolibrary.org/obo/UBERON_0000979",
    "ulna" -> "http://purl.obolibrary.org/obo/UBERON_0001424",
    "radial bone" -> "http://purl.obolibrary.org/obo/UBERON_2000271",
    "lepidotrichium" -> "http://purl.obolibrary.org/obo/UBERON_4000172",
    "forelimb long bone" -> "http://purl.obolibrary.org/obo/UBERON_0003607",
    "forelimb bone" -> "http://purl.obolibrary.org/obo/UBERON_0008962",
    "manus bone" -> "http://purl.obolibrary.org/obo/UBERON_0005897",
    "hindlimb long bone" -> "http://purl.obolibrary.org/obo/UBERON_0003608",
    "hindlimb bone" -> "http://purl.obolibrary.org/obo/UBERON_0003464",
    "pes bone" -> "http://purl.obolibrary.org/obo/UBERON_0005899",
    "limb bone" -> "http://purl.obolibrary.org/obo/UBERON_0002428",
    "limb" -> "http://purl.obolibrary.org/obo/UBERON_0002101",
    "digitopodium region" -> "http://purl.obolibrary.org/obo/UBERON_0012140",
    "pelvic fin" -> "http://purl.obolibrary.org/obo/UBERON_0000152",
    "pectoral fin" -> "http://purl.obolibrary.org/obo/UBERON_0000151",
    "paired fin" -> "http://purl.obolibrary.org/obo/UBERON_0002534",
    "paired limb/fin" -> "http://purl.obolibrary.org/obo/UBERON_0004708",
    "appendage girdle region" -> "http://purl.obolibrary.org/obo/UBERON_0007823",
    "appendage girdle complex" -> "http://purl.obolibrary.org/obo/UBERON_0010707",
    "girdle skeleton" -> "http://purl.obolibrary.org/obo/UBERON_0010719")

  def query(): Future[String] = {
    val results = for {
      (entityLabel, entityIRI) <- entities
    } yield {
      queryEntry(entityIRI).map { count =>
        s"\t$entityLabel\t$count"
      }
    }
    Future.sequence(results).map { entries =>
      entries.mkString("\n")
    }
  }

  private def queryEntry(entityIRI: String): Future[String] = {
    val query = buildQuery(entityIRI)
    for {
      results <- App.executeSPARQLQuery(query)
    } yield if (results.hasNext) results.next.getLiteral("count").getLexicalForm else "0"
  }

  //character states annotating the term or its parts
  def buildQuery(entityIRI: String): Query = {
    val entityClass = Class(IRI.create(entityIRI))
    val entityInd = Individual(entityIRI)
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, DENOTES_EXHIBITING / rdfType, 'phenotype),
        t('state, rdfType, STANDARD_STATE)),
        App.withOwlery(
          t('phenotype, rdfsSubClassOf, ((IMPLIES_PRESENCE_OF some entityClass) or (TOWARDS value entityInd)).asOMN)),
          App.BigdataRunPriorFirst)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("state"))))
    query
  }

}