package org.phenoscape.kb.resource

import java.io.BufferedWriter
import java.io.OutputStream
import java.io.OutputStreamWriter

import org.apache.log4j.Logger
import org.phenoscape.kb.util.App
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.owl.Vocab.DENOTES_EXHIBITING
import org.phenoscape.owl.Vocab.EXHIBITS
import org.phenoscape.owl.Vocab.HAS_MEMBER
import org.phenoscape.owl.Vocab.IMPLIES_PRESENCE_OF
import org.phenoscape.owl.Vocab.STANDARD_STATE
import org.phenoscape.owl.Vocab.TOWARDS
import org.phenoscape.owl.Vocab.rdfType
import org.phenoscape.scowl.OWL._
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLClassExpression

import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.core.Var
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.expr.aggregate.AggCountVarDistinct

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.Produces
import javax.ws.rs.client.ClientBuilder
import javax.ws.rs.core.Form
import javax.ws.rs.core.Response
import javax.ws.rs.core.StreamingOutput

@Path("report/data_coverage_figure_any_taxon")
class DataCoverageFigureReportAnyTaxon {

  private implicit val owlReasoner = App.reasoner
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

  @GET
  @Produces(Array("text/tab-separated-values"))
  def query(): Response = {
    val client = ClientBuilder.newClient()
    val target = client.target(App.endpoint)
    val stream = new StreamingOutput() {
      override def write(output: OutputStream) = {
        val writer = new BufferedWriter(new OutputStreamWriter(output))
        for ((entityLabel, entityIRI) <- entities) {
          val query = buildQuery(entityIRI)
          val queryEngine = new QueryEngineHTTP(App.endpoint, query);
          val resultSet = queryEngine.execSelect
          val result = if (resultSet.hasNext) resultSet.next.getLiteral("count") else "0"
          writer.write(s"$entityLabel\t$result\n")
          writer.flush()
        }
      }
    }
    Response.ok(stream).build()
  }

  //character states annotating the term or its parts
  def buildQuery(entityIRI: String): Query = {
    val entityClass = Class(IRI.create(entityIRI))
    val entityInd = Individual(entityIRI)
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('state, DENOTES_EXHIBITING / rdfType, 'phenotype),
        t('state, rdfType, STANDARD_STATE)),
        subClassOf('phenotype, (IMPLIES_PRESENCE_OF some entityClass) or (TOWARDS value entityInd)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("state"))))
    query
  }

  lazy val logger = Logger.getLogger(this.getClass)

}