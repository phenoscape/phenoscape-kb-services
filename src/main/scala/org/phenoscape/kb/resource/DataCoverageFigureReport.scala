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
import org.phenoscape.owlet.OwletManchesterSyntaxDataType._

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
import org.phenoscape.owl.Vocab._

@Path("report/data_coverage_figure")
class DataCoverageFigureReport {

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
    "lepidotrichium" -> "http://purl.obolibrary.org/obo/UBERON_4000172")

  val taxa = Set(
    "Acanthostega gunnari" -> "http://purl.obolibrary.org/obo/VTO_9001290",
    "Aztecia" -> "http://purl.obolibrary.org/obo/VTO_9022990",
    "Balanerpeton woodi" -> "http://purl.obolibrary.org/obo/VTO_9000671",
    "Baphetes" -> "http://purl.obolibrary.org/obo/VTO_9016432",
    "Baphetes kirkbyi" -> "http://purl.obolibrary.org/obo/VTO_9000762",
    "Baphetidae" -> "http://purl.obolibrary.org/obo/VTO_9007550",
    "Barameda" -> "http://purl.obolibrary.org/obo/VTO_9022956",
    "Barameda decipiens" -> "http://purl.obolibrary.org/obo/VTO_9002397",
    "Beelarongia" -> "http://purl.obolibrary.org/obo/VTO_9026226",
    "Cabonnichthys burnsi" -> "http://purl.obolibrary.org/obo/VTO_9034475",
    "Canowindra grossi" -> "http://purl.obolibrary.org/obo/VTO_9034461",
    "Capetus palustris" -> "http://purl.obolibrary.org/obo/VTO_9000702",
    "Chirodipterus" -> "http://purl.obolibrary.org/obo/VTO_9010969",
    "Cladarosymblema narrienense" -> "http://purl.obolibrary.org/obo/VTO_9034467",
    "Crassigyrinus scoticus" -> "http://purl.obolibrary.org/obo/VTO_9000765",
    "Densignathus" -> "http://purl.obolibrary.org/obo/VTO_9022020",
    "Diabolepis" -> "http://purl.obolibrary.org/obo/VTO_9010964",
    "Diploceraspis burkei" -> "http://purl.obolibrary.org/obo/VTO_9001328",
    "Diplocercides" -> "http://purl.obolibrary.org/obo/VTO_9011134",
    "Diplocercides heiligenstockiensis" -> "http://purl.obolibrary.org/obo/VTO_9026227",
    "Diplocercides kayseri" -> "http://purl.obolibrary.org/obo/VTO_9026228",
    "Dipnoi" -> "http://purl.obolibrary.org/obo/VTO_0033592",
    "Dipnorhynchus" -> "http://purl.obolibrary.org/obo/VTO_9010965",
    "Dipterus" -> "http://purl.obolibrary.org/obo/VTO_9010803",
    "Ectosteorhachis" -> "http://purl.obolibrary.org/obo/VTO_9011140",
    "Elginerpeton pancheni" -> "http://purl.obolibrary.org/obo/VTO_9032383",
    "Elpistostege" -> "http://purl.obolibrary.org/obo/VTO_9026225",
    "Eucritta melanolimnetes" -> "http://purl.obolibrary.org/obo/VTO_9000749",
    "Eusthenodon" -> "http://purl.obolibrary.org/obo/VTO_9032360",
    "Eusthenopteron foordi" -> "http://purl.obolibrary.org/obo/VTO_9000751",
    "Glyptolepis" -> "http://purl.obolibrary.org/obo/VTO_9011119",
    "Glyptopomus" -> "http://purl.obolibrary.org/obo/VTO_9008340",
    "Gogonasus andrewsae" -> "http://purl.obolibrary.org/obo/VTO_9032353",
    "Gooloogongia" -> "http://purl.obolibrary.org/obo/VTO_9022994",
    "Greererpeton burkemorani" -> "http://purl.obolibrary.org/obo/VTO_9001326",
    "Griphognathus" -> "http://purl.obolibrary.org/obo/VTO_9010794",
    "Guiyu oneiros" -> "http://purl.obolibrary.org/obo/VTO_9030864",
    "Gyroptychius" -> "http://purl.obolibrary.org/obo/VTO_9008342",
    "Holoptychius" -> "http://purl.obolibrary.org/obo/VTO_9033736",
    "Ichthyostega stensioei" -> "http://purl.obolibrary.org/obo/VTO_9000752",
    "Kenichthys" -> "http://purl.obolibrary.org/obo/VTO_9031588",
    "Latimeria" -> "http://purl.obolibrary.org/obo/VTO_0033619",
    "Mandageria fairfaxi" -> "http://purl.obolibrary.org/obo/VTO_9032336",
    "Medoevia" -> "http://purl.obolibrary.org/obo/VTO_9022802",
    "Marsdenichthys longioccipitus" -> "http://purl.obolibrary.org/obo/VTO_9032346",
    "Megalichthys" -> "http://purl.obolibrary.org/obo/VTO_9006420",
    "Neoceratodus" -> "http://purl.obolibrary.org/obo/VTO_0033614",
    "Onychodus" -> "http://purl.obolibrary.org/obo/VTO_9008350",
    "Ossinodus pueri" -> "http://purl.obolibrary.org/obo/VTO_9032794",
    "Osteolepis" -> "http://purl.obolibrary.org/obo/VTO_9008377",
    "Panderichthys rhombolepis" -> "http://purl.obolibrary.org/obo/VTO_9000724",
    "Pederpes finneyae" -> "http://purl.obolibrary.org/obo/VTO_9031042",
    "Phaneropleuron" -> "http://purl.obolibrary.org/obo/VTO_9007375",
    "Platycephalichthys" -> "http://purl.obolibrary.org/obo/VTO_9032995",
    "Porolepis" -> "http://purl.obolibrary.org/obo/VTO_9008373",
    "Powichthys" -> "http://purl.obolibrary.org/obo/VTO_9008372",
    "Psarolepis" -> "http://purl.obolibrary.org/obo/VTO_9021132",
    "Rhizodus" -> "http://purl.obolibrary.org/obo/VTO_9008365",
    "Rhizodopsis" -> "http://purl.obolibrary.org/obo/VTO_9004049",
    "Sauripterus taylori" -> "http://purl.obolibrary.org/obo/VTO_9011673",
    "Screbinodus" -> "http://purl.obolibrary.org/obo/VTO_9022795",
    "Speonesydrion" -> "http://purl.obolibrary.org/obo/VTO_9026879",
    "Strepsodus" -> "http://purl.obolibrary.org/obo/VTO_9022804",
    "Spodichthys buetleri" -> "http://purl.obolibrary.org/obo/VTO_9033057",
    "Strunius" -> "http://purl.obolibrary.org/obo/VTO_9008395",
    "Styloichthys" -> "http://purl.obolibrary.org/obo/VTO_9033408",
    "Tiktaalik roseae" -> "http://purl.obolibrary.org/obo/VTO_9000793",
    "Tinirau clackae" -> "http://purl.obolibrary.org/obo/VTO_9034469",
    "Tristichopterus alatus" -> "http://purl.obolibrary.org/obo/VTO_9033017",
    "Uranolophus" -> "http://purl.obolibrary.org/obo/VTO_9010782",
    "Uronemus" -> "http://purl.obolibrary.org/obo/VTO_9001389",
    "Ventastega curonica" -> "http://purl.obolibrary.org/obo/VTO_9027749",
    "Westlothiana lizziae" -> "http://purl.obolibrary.org/obo/VTO_9031047",
    "Whatcheeria deltae" -> "http://purl.obolibrary.org/obo/VTO_9019883",
    "Whatcheeriidae" -> "http://purl.obolibrary.org/obo/VTO_9031049",
    "Youngolepis" -> "http://purl.obolibrary.org/obo/VTO_9008383")

  @GET
  @Produces(Array("text/tab-separated-values"))
  def query(): Response = {
    val client = ClientBuilder.newClient()
    val target = client.target(App.endpoint)
    val stream = new StreamingOutput() {
      override def write(output: OutputStream) = {
        val writer = new BufferedWriter(new OutputStreamWriter(output))
        for {
          (entityLabel, entityIRI) <- entities
          (taxonLabel, taxonIRI) <- taxa
        } {
          val query = buildQuery(Class(taxonIRI), entityIRI)
          val queryEngine = new QueryEngineHTTP(App.endpoint, query);
          val resultSet = queryEngine.execSelect
          val result = if (resultSet.hasNext) resultSet.next.getLiteral("count") else "0"
          writer.write(s"$taxonLabel\t$entityLabel\t$result\n")
          writer.flush()
        }
      }
    }
    Response.ok(stream).build()
  }

  //character states annotating the term or its parts
  def buildQuery(taxonClass: OWLClassExpression, entityIRI: String): Query = {
    val entityClass = Class(IRI.create(entityIRI))
    val entityInd = Individual(entityIRI)
    val query = select() from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, HAS_MEMBER / EXHIBITS / rdfType, 'phenotype),
        t('state, DENOTES_EXHIBITING / rdfType, 'phenotype),
        t('state, rdfType, STANDARD_STATE)),
        service(App.owlery, bgp(
          t('taxon, rdfsSubClassOf, taxonClass.asOMN))),
        service(App.owlery, bgp(
          t('phenotype, rdfsSubClassOf, ((IMPLIES_PRESENCE_OF some entityClass) or (TOWARDS value entityInd)).asOMN))),
        App.BigdataRunPriorFirst)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("state"))))
    query
  }

  lazy val logger = Logger.getLogger(this.getClass)

}