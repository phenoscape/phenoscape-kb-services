package org.phenoscape.kb

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.apache.log4j.Logger
import org.phenoscape.kb.App.withOwlery
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl.OWL._
import org.phenoscape.kb.KBVocab._
import org.phenoscape.kb.KBVocab.rdfsLabel
import org.phenoscape.kb.KBVocab.rdfsSubClassOf
import org.semanticweb.owlapi.model.IRI
import org.semanticweb.owlapi.model.OWLEntity
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QuerySolution
import org.semanticweb.owlapi.model.OWLClassExpression
import org.phenoscape.model.DataSet
import com.hp.hpl.jena.sparql.path.P_Link
import com.hp.hpl.jena.sparql.expr.ExprList
import com.hp.hpl.jena.sparql.expr.ExprVar
import com.hp.hpl.jena.sparql.expr.E_OneOf
import com.hp.hpl.jena.sparql.syntax.ElementFilter
import com.hp.hpl.jena.sparql.expr.nodevalue.NodeValueNode
import scala.collection.JavaConversions._
import scala.collection.mutable
import org.phenoscape.model.State
import org.phenoscape.model.MultipleState
import org.phenoscape.model.MultipleState.MODE
import org.phenoscape.model.Character
import org.phenoscape.model.{ Taxon => MatrixTaxon }
import org.phenoscape.io.NeXMLUtil
import com.hp.hpl.jena.rdf.model.Statement
import com.hp.hpl.jena.rdf.model.Property
import com.hp.hpl.jena.rdf.model.ResourceFactory
import scala.language.implicitConversions
import org.phenoscape.owl.util.OBOUtil
import java.net.URI
import org.obo.datamodel.impl.OBOClassImpl
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.language.postfixOps

object PresenceAbsenceOfStructure {

  implicit def owlEntityToJenaProperty(prop: OWLEntity): Property = ResourceFactory.createProperty(prop.getIRI.toString)

  def statesEntailingAbsence(taxon: IRI, entity: IRI): Future[String] =
    App.executeSPARQLQuery(buildAbsenceQuery(taxon, entity)).map(App.resultSetToTSV(_))

  def statesEntailingPresence(taxon: IRI, entity: IRI): Future[String] =
    App.executeSPARQLQuery(buildPresenceQuery(taxon, entity)).map(App.resultSetToTSV(_))

  def taxaExhibitingPresence(entity: IRI, limit: Int): Future[Seq[Taxon]] = {
    App.executeSPARQLQuery(buildExhibitingPresenceQuery(entity, limit), resultToTaxon)
  }

  def taxaExhibitingAbsence(entity: IRI, limit: Int): Future[Seq[Taxon]] =
    App.executeSPARQLQuery(buildExhibitingAbsenceQuery(entity, limit), resultToTaxon)

  def presenceAbsenceMatrix(entityClass: OWLClassExpression, taxonClass: OWLClassExpression, variableOnly: Boolean): Future[DataSet] = for {
    query <- App.expandWithOwlet(buildMatrixQuery(entityClass, taxonClass))
    model <- App.executeSPARQLConstructQuery(query)
  } yield {
    val dataset = new DataSet()
    val characters: mutable.Map[String, Character] = mutable.Map()
    val states: mutable.Map[String, State] = mutable.Map()
    val taxa: mutable.Map[String, MatrixTaxon] = mutable.Map()
    val presencesAndAbsences = {
      val allPresences = model.listStatements(null, has_presence_of, null).toSet
      val allAbsences = model.listStatements(null, has_absence_of, null).toSet
      val allStatements = allPresences ++ allAbsences
      if (variableOnly) {
        val presentEntities = model.listObjectsOfProperty(has_presence_of).toSet
        val absentEntities = model.listObjectsOfProperty(has_absence_of).toSet
        val variableEntities = absentEntities & presentEntities
        allStatements.filter(variableEntities contains _.getObject)
      } else {
        allStatements
      }
    }
    for (statement <- presencesAndAbsences) {
      val taxon = statement.getSubject
      val entity = statement.getObject.asResource
      val presenceAbsence: PresenceAbsence = if (statement.getPredicate.getURI == has_presence_of.getIRI.toString) Presence else Absence
      val character = characters.getOrElseUpdate(entity.getURI, {
        val newChar = new Character(entity.getURI)
        newChar.setLabel(model.getProperty(entity, rdfsLabel).getObject.asLiteral.getString)
        dataset.addCharacter(newChar)
        newChar
      })
      val stateID = s"${entity.getURI}#${presenceAbsence.symbol}"
      val state = states.getOrElseUpdate(stateID, {
        val newState = new State(stateID)
        newState.setSymbol(presenceAbsence.symbol)
        newState.setLabel(presenceAbsence.label)
        newState
      })
      if (!character.getStates.contains(state)) character.addState(state)
      val matrixTaxon = taxa.getOrElseUpdate(taxon.getURI, {
        val newTaxon = new MatrixTaxon(taxon.getURI)
        newTaxon.setPublicationName(model.getProperty(taxon, rdfsLabel).getObject.asLiteral.getString)
        val oboID = NeXMLUtil.oboID(URI.create(taxon.getURI))
        newTaxon.setValidName(new OBOClassImpl(oboID))
        dataset.addTaxon(newTaxon)
        newTaxon
      })
      val currentState = dataset.getStateForTaxon(matrixTaxon, character)
      val stateToAssign = currentState match {
        case polymorphic: MultipleState => addStateToMultiState(polymorphic, state)
        case `state`                    => state
        case null                       => state
        case _                          => new MultipleState(Set(currentState, state), MODE.POLYMORPHIC)
      }
      dataset.setStateForTaxon(matrixTaxon, character, stateToAssign)
    }
    val date = new SimpleDateFormat("y-M-d").format(Calendar.getInstance.getTime)
    dataset.setPublicationNotes(s"Generated from the Phenoscape Knowledgebase on $date by Ontotrace query:\n* taxa: ${taxonClass.asOMN.getLiteralLexicalForm}\n* entities: ${entityClass.asOMN.getLiteralLexicalForm}")
    dataset
  }

  def buildMatrixQuery(entityClass: OWLClassExpression, taxonClass: OWLClassExpression): Query = {
    construct(
      t('taxon, 'relation, 'entity),
      t('taxon, rdfsLabel, 'taxon_label),
      t('entity, rdfsLabel, 'entity_label)) from "http://kb.phenoscape.org/" where (
        bgp(
          t('taxon, 'relation, 'entity),
          t('taxon, rdfsLabel, 'taxon_label),
          t('entity, rdfsLabel, 'entity_label),
          t('entity, rdfsSubClassOf, entityClass.asOMN),
          t('taxon, rdfsSubClassOf, taxonClass.asOMN)),
          new ElementFilter(new E_OneOf(new ExprVar('relation),
            new ExprList(List(new NodeValueNode(has_presence_of), new NodeValueNode(has_absence_of))))))
  }

  def expandMatrixQuery(query: Query): Future[Query] = {
    App.expandWithOwlet(query)
  }

  def buildAbsenceQuery(taxonIRI: IRI, entityIRI: IRI): Query = {
    val taxon = Class(taxonIRI)
    val entity = Individual(entityIRI)
    select_distinct('state, 'state_label, 'matrix_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(taxon, exhibits_state, 'state),
        t('state, describes_phenotype, 'phenotype),
        t('state, dcDescription, 'state_label),
        t('matrix, has_character, 'matrix_char),
        t('matrix, rdfsLabel, 'matrix_label),
        t('matrix_char, may_have_state_value, 'state)),
        withOwlery(
          t('phenotype, owlEquivalentClass | rdfsSubClassOf, (LacksAllPartsOfType and (towards value entity) and (inheres_in some MultiCellularOrganism)).asOMN)),
          App.BigdataRunPriorFirst)
  }

  def buildPresenceQuery(taxonIRI: IRI, entityIRI: IRI): Query = {
    val taxon = Class(taxonIRI)
    val entity = Class(entityIRI)
    select_distinct('state, 'state_label, 'matrix_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t(taxon, exhibits_state, 'state),
        t('state, describes_phenotype, 'phenotype),
        t('state, dcDescription, 'state_label),
        t('matrix, has_character, 'matrix_char),
        t('matrix, rdfsLabel, 'matrix_label),
        t('matrix_char, may_have_state_value, 'state)),
        withOwlery(
          t('phenotype, owlEquivalentClass | rdfsSubClassOf, (IMPLIES_PRESENCE_OF some entity).asOMN)),
          App.BigdataRunPriorFirst)
  }

  def buildExhibitingAbsenceQuery(entityIRI: IRI, limit: Int): Query = {
    val query = select_distinct('taxon, 'taxon_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, has_absence_of, entityIRI),
        t('taxon, rdfsLabel, 'taxon_label)))
    query.setLimit(limit)
    query
  }

  def buildExhibitingPresenceQuery(entityIRI: IRI, limit: Int): Query = {
    val entity = Class(entityIRI)
    val query = select_distinct('taxon, 'taxon_label) from "http://kb.phenoscape.org/" where (
      bgp(
        t('taxon, has_presence_of, entityIRI),
        t('taxon, rdfsLabel, 'taxon_label)))
    query.setLimit(limit)
    query
  }

  private def resultToTaxon(result: QuerySolution): Taxon = Taxon(
    IRI.create(result.getResource("taxon").getURI),
    result.getLiteral("taxon_label").getLexicalForm)

  private def addStateToMultiState(multi: MultipleState, state: State): MultipleState = {
    if (multi.getStates.map(_.getNexmlID).contains(state.getNexmlID)) multi
    else new MultipleState(multi.getStates + state, multi.getMode)
  }

  private lazy val logger = Logger.getLogger(this.getClass)

}

case class Association(entity: String, entityLabel: String, taxon: String, taxonLabel: String, state: String, stateLabel: String, matrixLabel: String, direct: Boolean)

sealed abstract class PresenceAbsence(val symbol: String, val label: String)
case object Absence extends PresenceAbsence("0", "absent")
case object Presence extends PresenceAbsence("1", "present")

