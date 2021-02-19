package org.phenoscape.kb

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.jena.query.{Query, QuerySolution}
import org.apache.jena.rdf.model.{Property, ResourceFactory}
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.expr.aggregate.{AggCountDistinct, AggCountVarDistinct}
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode
import org.apache.jena.sparql.expr.{E_OneOf, Expr, ExprList, ExprVar}
import org.apache.jena.sparql.syntax.{ElementFilter, ElementSubQuery}
import org.apache.log4j.Logger
import org.obo.datamodel.impl.OBOClassImpl
import org.phenoscape.io.NeXMLUtil
import org.phenoscape.kb.KBVocab.{owlNothing, rdfsLabel, rdfsSubClassOf}
import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.model.MultipleState.MODE
import org.phenoscape.model.{Character, DataSet, MultipleState, State, Taxon => MatrixTaxon}
import org.phenoscape.owl.NamedRestrictionGenerator
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.scowl._
import org.semanticweb.owlapi.model.{IRI, OWLClassExpression, OWLEntity}
import uk.ac.manchester.cs.owl.owlapi.OWLClassImpl
import org.phenoscape.sparql.SPARQLInterpolation._
import org.phenoscape.sparql.SPARQLInterpolationOWL._
import org.phenoscape.kb.util.SPARQLInterpolatorOWLAPI._
import org.phenoscape.kb.KBVocab.KBMainGraph
import scalaz.NonEmptyList

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.Future
import scala.language.{implicitConversions, postfixOps}

object PresenceAbsenceOfStructure {

  val implies_presence_of_some = NamedRestrictionGenerator.getClassRelationIRI(IMPLIES_PRESENCE_OF.getIRI)

  implicit def owlEntityToJenaProperty(prop: OWLEntity): Property = ResourceFactory.createProperty(prop.getIRI.toString)

  def statesEntailingAbsence(taxon: IRI,
                             entity: IRI,
                             limit: Int = 20,
                             offset: Int = 0): Future[Seq[AnnotatedCharacterDescription]] =
    App
      .executeSPARQLQuery(buildAbsenceStatesQuery(taxon, entity, limit, offset),
                          AnnotatedCharacterDescription.fromQuerySolution)
      .flatMap(Future.sequence(_))

  def statesEntailingAbsenceTotal(taxon: IRI, entity: IRI): Future[Int] =
    App.executeSPARQLQuery(buildAbsenceStatesQueryTotal(taxon, entity)).map(ResultCount.count)

  def statesEntailingPresence(taxon: IRI,
                              entity: IRI,
                              limit: Int = 20,
                              offset: Int = 0): Future[Seq[AnnotatedCharacterDescription]] =
    App
      .executeSPARQLQuery(buildPresenceStatesQuery(taxon, entity, limit, offset),
                          AnnotatedCharacterDescription.fromQuerySolution)
      .flatMap(Future.sequence(_))

  def statesEntailingPresenceTotal(taxon: IRI, entity: IRI): Future[Int] =
    App.executeSPARQLQuery(buildPresenceStatesQueryTotal(taxon, entity)).map(ResultCount.count)

  def taxaExhibitingPresence(entity: IRI,
                             taxonFilter: Option[IRI],
                             limit: Int = 20,
                             offset: Int = 0): Future[Seq[Taxon]] =
    App.executeSPARQLQuery(buildExhibitingPresenceQuery(entity, taxonFilter, limit, offset), resultToTaxon)

  def taxaExhibitingPresenceTotal(entity: IRI, taxonFilter: Option[IRI]): Future[Int] =
    App.executeSPARQLQuery(buildExhibitingPresenceTotalQuery(entity, taxonFilter)).map(ResultCount.count)

  def taxaExhibitingAbsence(entity: IRI,
                            taxonFilter: Option[IRI],
                            limit: Int = 20,
                            offset: Int = 0): Future[Seq[Taxon]] =
    App.executeSPARQLQuery(buildExhibitingAbsenceQuery(entity, taxonFilter, limit, offset), resultToTaxon)

  def taxaExhibitingAbsenceTotal(entity: IRI, taxonFilter: Option[IRI]): Future[Int] =
    App.executeSPARQLQuery(buildExhibitingAbsenceTotalQuery(entity, taxonFilter)).map(ResultCount.count)

  def presenceAbsenceMatrix(mainEntityClassOpt: Option[OWLClassExpression],
                            entityList: Option[NonEmptyList[IRI]],
                            taxonClassExpr: Option[OWLClassExpression],
                            taxonList: Option[NonEmptyList[IRI]],
                            variableOnly: Boolean,
                            includeParts: Boolean): Future[DataSet] = {
    val entityClass =
      mainEntityClassOpt.map(classExpr => if (includeParts) classExpr or (part_of some classExpr) else classExpr)

    val buildDateFut = KB.buildDate
    for {
      query <- App.expandWithOwlet(
        buildMatrixQuery(entityClass, entityList, taxonClassExpr, taxonList)
      )
      model <- App.executeSPARQLConstructQuery(query)
      buildDate <- buildDateFut
    } yield {
      val dataset = new DataSet()
      dataset.setCurators("Phenoscape KB built on " + buildDate.toString)
      val characters: mutable.Map[String, Character] = mutable.Map()
      val states: mutable.Map[String, State] = mutable.Map()
      val taxa: mutable.Map[String, MatrixTaxon] = mutable.Map()
      val presencesAndAbsences = {
        val allPresences = model.listStatements(null, has_presence_of, null).asScala.toSet
        val allAbsences = model.listStatements(null, has_absence_of, null).asScala.toSet
        val allStatements = allPresences ++ allAbsences
        if (variableOnly) {
          val presentEntities = model.listObjectsOfProperty(has_presence_of).asScala.toSet
          val absentEntities = model.listObjectsOfProperty(has_absence_of).asScala.toSet
          val variableEntities = absentEntities & presentEntities
          allStatements.filter(s => variableEntities(s.getObject))
        } else
          allStatements
      }
      for (statement <- presencesAndAbsences) {
        val taxon = statement.getSubject
        val entity = statement.getObject.asResource
        val presenceAbsence: PresenceAbsence =
          if (statement.getPredicate.getURI == has_presence_of.getIRI.toString) Presence else Absence
        val characterID = unOBO(entity.getURI)
        val character = characters.getOrElseUpdate(
          characterID, {
            val newChar = new Character(characterID)
            newChar.setLabel(model.getProperty(entity, rdfsLabel).getObject.asLiteral.getString)
            newChar.setDenotes(URI.create(entity.getURI))
            dataset.addCharacter(newChar)
            newChar
          }
        )
        val stateID = s"${unOBO(entity.getURI)}_${presenceAbsence.symbol}"
        val state = states.getOrElseUpdate(stateID, {
                                             val newState = new State(stateID)
                                             newState.setSymbol(presenceAbsence.symbol)
                                             newState.setLabel(presenceAbsence.label)
                                             newState
                                           })
        if (!character.getStates.contains(state)) character.addState(state)
        val matrixTaxonID = unOBO(taxon.getURI)
        val matrixTaxon = taxa.getOrElseUpdate(
          matrixTaxonID, {
            val newTaxon = new MatrixTaxon(matrixTaxonID)
            newTaxon.setPublicationName(model.getProperty(taxon, rdfsLabel).getObject.asLiteral.getString)
            val oboID = NeXMLUtil.oboID(URI.create(taxon.getURI))
            newTaxon.setValidName(new OBOClassImpl(oboID))
            dataset.addTaxon(newTaxon)
            newTaxon
          }
        )
        val currentState = dataset.getStateForTaxon(matrixTaxon, character)
        val stateToAssign = currentState match {
          case polymorphic: MultipleState => addStateToMultiState(polymorphic, state)
          case `state`                    => state
          case null                       => state
          case _                          => new MultipleState(Set(currentState, state).asJava, MODE.POLYMORPHIC)
        }
        dataset.setStateForTaxon(matrixTaxon, character, stateToAssign)
      }
      val date = new SimpleDateFormat("y-M-d").format(Calendar.getInstance.getTime)
      dataset.setPublicationNotes(
        s"Generated from the Phenoscape Knowledgebase on $date by Ontotrace query :\n* taxa: ${taxonClassExpr
          .map(p => p.asOMN.getLiteralLexicalForm)
          .getOrElse("Not provided")}\n* taxa list: ${taxonList.map(_.list).getOrElse("Not provided")} \n* entities: ${mainEntityClassOpt
          .map(p => p.asOMN.getLiteralLexicalForm)
          .getOrElse("Not provided")}\n* entity list: ${entityList.map(_.list).getOrElse("Not provided")} \n* variable only: ${variableOnly}"
      )
      dataset
    }
  }

  private def unOBO(uri: String): String = uri.replaceAllLiterally("http://purl.obolibrary.org/obo/", "")

  def buildMatrixQuery(entityClass: Option[OWLClassExpression],
                       entityList: Option[NonEmptyList[IRI]],
                       taxonClass: Option[OWLClassExpression],
                       taxonList: Option[NonEmptyList[IRI]]): Query = {
    val entityExpressionQueryOpt = entityClass.map(cls => sparql" ?entity $rdfsSubClassOf ${cls.asOMN} . ")
    val entityValuesOpt = entityList.flatMap(list => list.map(iri => sparql" $iri ").list.reduceLeftOption(_ + _)).map {
      iris =>
        sparql"VALUES ?entity { $iris }"
    }
    val entityQuery = (entityExpressionQueryOpt, entityValuesOpt) match {
      case (Some(expressionQuery), Some(entityValues)) => sparql"{ $expressionQuery } UNION { $entityValues } "
      case _                                           => entityExpressionQueryOpt.orElse(entityValuesOpt).getOrElse(sparql"")
    }
    val taxonExpressionQueryOpt = taxonClass.map(cls => sparql" ?taxon $rdfsSubClassOf ${cls.asOMN} . ")
    val taxonValuesOpt = taxonList.flatMap(list => list.map(iri => sparql" $iri ").list.reduceLeftOption(_ + _)).map {
      iris =>
        sparql"VALUES ?taxon { $iris }"
    }
    val taxonQuery = (taxonExpressionQueryOpt, taxonValuesOpt) match {
      case (Some(expressionQuery), Some(taxonValues)) => sparql"{ $expressionQuery } UNION { $taxonValues } "
      case _                                          => taxonExpressionQueryOpt.orElse(taxonValuesOpt).getOrElse(sparql"")
    }
    val graphPattern =
      sparql"""
              ?taxon ?relation ?entity .
              ?taxon $rdfsLabel ?taxon_label .
              ?entity $rdfsLabel ?entity_label .
            """

    val query: QueryText =
      sparql"""
               CONSTRUCT {
                $graphPattern
                }
               FROM  $KBMainGraph
               WHERE {
                  $graphPattern
                  $entityQuery
                  $taxonQuery
                  VALUES ?relation {$has_presence_of $has_absence_of}
               }
            """
    query.toQuery
  }

  def expandMatrixQuery(query: Query): Future[Query] =
    App.expandWithOwlet(query)

  def buildAbsenceStatesQuery(taxonIRI: IRI, entityIRI: IRI, limit: Int, offset: Int): Query = {
    val query =
      buildAbsenceStatesQueryBase(taxonIRI,
                                  entityIRI) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure"
    if (limit > 1) {
      query.setOffset(offset)
      query.setLimit(limit)
    }
    query.addOrderBy('description)
    query.addOrderBy('phenotype)
    query
  }

  def buildAbsenceStatesQueryTotal(taxonIRI: IRI, entityIRI: IRI): Query = {
    val query =
      select() from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where (new ElementSubQuery(
        buildAbsenceStatesQueryBase(taxonIRI, entityIRI)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountDistinct()))
    query
  }

  def buildAbsenceStatesQueryBase(taxonIRI: IRI, entityIRI: IRI): Query =
    select_distinct('phenotype, 'state, 'description, 'matrix, 'matrix_label, 'character, 'character_label) where (
      bgp(
        t(taxonIRI, exhibits_state, 'state),
        t('state, describes_phenotype, 'phenotype),
        t('phenotype, rdfsSubClassOf / ABSENCE_OF, entityIRI), //FIXME needs to inhere in organism
        t('state, dcDescription, 'description),
        t('matrix, has_character, 'character),
        t('character, rdfsLabel, 'character_label),
        t('matrix, rdfsLabel, 'matrix_label),
        t('character, may_have_state_value, 'state)
      )
    )

  def buildPresenceStatesQuery(taxonIRI: IRI, entityIRI: IRI, limit: Int, offset: Int): Query = {
    val query =
      buildPresenceStatesQueryBase(taxonIRI,
                                   entityIRI) from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure"
    if (limit > 1) {
      query.setOffset(offset)
      query.setLimit(limit)
    }
    query.addOrderBy('description)
    query.addOrderBy('phenotype)
    query
  }

  def buildPresenceStatesQueryTotal(taxonIRI: IRI, entityIRI: IRI): Query = {
    val query =
      select() from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where (new ElementSubQuery(
        buildPresenceStatesQueryBase(taxonIRI, entityIRI)))
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountDistinct()))
    query
  }

  def buildPresenceStatesQueryBase(taxonIRI: IRI, entityIRI: IRI): Query =
    select_distinct('phenotype, 'state, 'description, 'matrix, 'matrix_label, 'character, 'character_label) where (
      bgp(
        t(taxonIRI, exhibits_state, 'state),
        t('state, describes_phenotype, 'phenotype),
        t('phenotype, rdfsSubClassOf / implies_presence_of_some, entityIRI),
        t('state, dcDescription, 'description),
        t('matrix, has_character, 'character),
        t('character, rdfsLabel, 'character_label),
        t('matrix, rdfsLabel, 'matrix_label),
        t('character, may_have_state_value, 'state)
      )
    )

  def buildExhibitingAbsenceBasicQuery(entityIRI: IRI, taxonFilter: Option[IRI]): Query = {
    val taxonFilterTriple = taxonFilter.map(t('taxon, rdfsSubClassOf, _)).toList
    select_distinct() from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where (bgp(
      (App.BigdataAnalyticQuery ::
        t('taxon, has_absence_of, entityIRI) ::
        t('taxon, rdfsLabel, 'taxon_label) ::
        taxonFilterTriple): _*))
  }

  def buildExhibitingAbsenceQuery(entityIRI: IRI, taxonFilter: Option[IRI], limit: Int, offset: Int): Query = {
    val query = buildExhibitingAbsenceBasicQuery(entityIRI, taxonFilter)
    query.addResultVar('taxon)
    query.addResultVar('taxon_label)
    query.setOffset(offset)
    if (limit > 0) query.setLimit(limit)
    query.addOrderBy('taxon_label)
    query.addOrderBy('taxon)
    query
  }

  def buildExhibitingAbsenceTotalQuery(entityIRI: IRI, taxonFilter: Option[IRI]): Query = {
    val query = buildExhibitingAbsenceBasicQuery(entityIRI, taxonFilter)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("taxon"))))
    query
  }

  def buildExhibitingPresenceBasicQuery(entityIRI: IRI, taxonFilter: Option[IRI]): Query = {
    val taxonFilterTriple = taxonFilter.map(t('taxon, rdfsSubClassOf, _)).toList
    select_distinct() from "http://kb.phenoscape.org/" from "http://kb.phenoscape.org/closure" where (bgp(
      (App.BigdataAnalyticQuery ::
        t('taxon, has_presence_of, entityIRI) ::
        t('taxon, rdfsLabel, 'taxon_label) ::
        taxonFilterTriple): _*))
  }

  def buildExhibitingPresenceQuery(entityIRI: IRI, taxonFilter: Option[IRI], limit: Int, offset: Int): Query = {
    val query = buildExhibitingPresenceBasicQuery(entityIRI, taxonFilter)
    query.addResultVar('taxon)
    query.addResultVar('taxon_label)
    query.setOffset(offset)
    if (limit > 0) query.setLimit(limit)
    query.addOrderBy('taxon_label)
    query.addOrderBy('taxon)
    query
  }

  def buildExhibitingPresenceTotalQuery(entityIRI: IRI, taxonFilter: Option[IRI]): Query = {
    val query = buildExhibitingPresenceBasicQuery(entityIRI, taxonFilter)
    query.getProject.add(Var.alloc("count"), query.allocAggregate(new AggCountVarDistinct(new ExprVar("taxon"))))
    query
  }

  private def resultToTaxon(result: QuerySolution): Taxon =
    Taxon(IRI.create(result.getResource("taxon").getURI), result.getLiteral("taxon_label").getLexicalForm)

  private def addStateToMultiState(multi: MultipleState, state: State): MultipleState =
    if (multi.getStates.asScala.map(_.getNexmlID).contains(state.getNexmlID)) multi
    else new MultipleState((multi.getStates.asScala + state).asJava, multi.getMode)

  private lazy val logger = Logger.getLogger(this.getClass)

}

case class Association(entity: String,
                       entityLabel: String,
                       taxon: String,
                       taxonLabel: String,
                       state: String,
                       stateLabel: String,
                       matrixLabel: String,
                       direct: Boolean)

sealed abstract class PresenceAbsence(val symbol: String, val label: String)

case object Absence extends PresenceAbsence("0", "absent")

case object Presence extends PresenceAbsence("1", "present")
