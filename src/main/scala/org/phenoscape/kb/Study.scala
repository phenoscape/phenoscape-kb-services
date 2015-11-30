package org.phenoscape.kb

import org.phenoscape.kb.Main.system.dispatcher
import org.phenoscape.scowl.OWL._
import org.phenoscape.owl.Vocab._
import org.phenoscape.owlet.SPARQLComposer._
import org.phenoscape.owlet.OwletManchesterSyntaxDataType.SerializableClassExpression
import org.semanticweb.owlapi.model.OWLClassExpression
import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.query.QuerySolution
import org.semanticweb.owlapi.model.IRI
import scala.concurrent.Future
import java.util.UUID
import scala.xml.NodeSeq
import com.hp.hpl.jena.rdf.model.Model
import scala.xml.Elem
import com.hp.hpl.jena.rdf.model.Property
import org.semanticweb.owlapi.model.OWLProperty
import org.semanticweb.owlapi.model.OWLObjectProperty
import com.hp.hpl.jena.graph.NodeFactory
import com.hp.hpl.jena.rdf.model.ResourceFactory
import scala.language.implicitConversions
import org.semanticweb.owlapi.model.OWLEntity
import scala.collection.JavaConversions._

object Study {

  val AnatomicalEntity = Class(ANATOMICAL_ENTITY)
  val Chordata = Class(CHORDATA)

  def queryStudies(entityOpt: Option[OWLClassExpression], taxonOpt: Option[OWLClassExpression]): Future[Seq[MinimalTerm]] = for {
    query <- App.expandWithOwlet(buildQuery(entityOpt, taxonOpt))
    studies <- App.executeSPARQLQuery(query, queryToTerm)
  } yield {
    studies
  }

  def buildQuery(entityOpt: Option[OWLClassExpression], taxonOpt: Option[OWLClassExpression]): Query = {
    val entityPatterns = entityOpt.toList.flatMap(entity =>
      t('study, has_character / may_have_state_value / describes_phenotype, 'phenotype) ::
        t('phenotype, rdfsSubClassOf, (phenotype_of some entity).asOMN) :: Nil)
    val taxonPatterns = taxonOpt.toList.flatMap(taxon =>
      t('study, has_TU / has_external_reference, 'taxon) ::
        t('taxon, rdfsSubClassOf, taxon.asOMN) :: Nil)
    val query = select_distinct('study, 'study_label) where (
      bgp(
        (t('study, rdfType, CharacterStateDataMatrix) ::
          t('study, rdfsLabel, 'study_label) ::
          entityPatterns ++ taxonPatterns): _*))

    query.addOrderBy('study_label)
    query.addOrderBy('study)
    query
  }

  //FIXME add to Vocab in phenoscape-owl-tools
  private val dcBibliographicCitation = factory.getOWLAnnotationProperty(IRI.create("http://rs.tdwg.org/dwc/terms/bibliographicCitation"))

  def queryMatrix(study: IRI): Future[Elem] = {
    val pattern = Seq(
      t(study, rdfsLabel, 'study_label),
      t(study, dcBibliographicCitation, 'citation),
      t(study, has_character, 'character),
      t('character, rdfsLabel, 'character_label),
      t('character, list_index, 'character_num),
      t(study, has_TU, 'otu),
      t('otu, rdfsLabel, 'otu_label),
      t('otu, has_external_reference, 'taxon),
      t('taxon, rdfsLabel, 'taxon_label),
      t('character, may_have_state_value, 'state),
      t('state, rdfsLabel, 'state_label),
      t('state, state_symbol, 'state_symbol),
      t('state, describes_phenotype, 'phenotype),
      //t('phenotype, rdfsLabel, 'phenotype_label),
      t('cell, belongs_to_character, 'character),
      t('cell, belongs_to_TU, 'otu),
      t('cell, has_state, 'state))
    val query = construct(pattern: _*) from "http://kb.phenoscape.org/" where (
      bgp(pattern: _*))
    val modelFuture = App.executeSPARQLConstructQuery(query)
    modelFuture.map(matrix(study, _))
  }

  def matrix(study: IRI, model: Model) = { //FIXME add about=#
    val otusID = s"otus_${UUID.randomUUID.toString}"
    val otuToID = (for {
      otu <- model.listObjectsOfProperty(has_TU)
    } yield otu.asResource.getURI -> s"otu_${UUID.randomUUID.toString}").toMap
    def idForStateGroup(states: Set[String]): String =
      if (states.size == 1) s"state_${UUID.randomUUID.toString}"
      else states.map(state => idForStateGroup(Set(state))).toSeq.sorted.mkString("_")
    val characterToIDSuffix = (for {
      character <- model.listObjectsOfProperty(has_character)
    } yield character.asResource.getURI -> UUID.randomUUID.toString).toMap
    val neededStateGroups = (for {
      character <- model.listObjectsOfProperty(has_character)
      stateGroup <- model.listSubjectsWithProperty(belongs_to_character, character)
        .map(cell => model.listObjectsOfProperty(cell.asResource, has_state).map(_.asResource.getURI))
    } yield character.asResource.getURI -> stateGroup.toSet).toSet[(String, Set[String])].groupBy(_._1).mapValues(_.map(_._2))
    val allIndividualStatesAsSets = neededStateGroups.values.flatten.flatten.map(Set(_))
    val stateGroupIDs = (for {
      (character, stateGroups) <- neededStateGroups
      stateGroup <- stateGroups
    } yield stateGroup -> idForStateGroup(stateGroup)) ++ allIndividualStatesAsSets.map(group => group -> idForStateGroup(group))
    val orderedCharacters = model.listObjectsOfProperty(has_character).toSeq.sortBy(character => model.getProperty(character.asResource, list_index).getInt).map(_.asResource.getURI)
    def label(uri: String): String = model.getProperty(ResourceFactory.createResource(uri), rdfsLabel).getString
    def symbol(stateGroup: Set[String]): String = (stateGroup.map(state => model.getProperty(ResourceFactory.createResource(state), state_symbol).getString)).toSeq.sorted.mkString(" and ")

    <nexml xmlns="http://www.nexml.org/2009" xmlns:dc="http://purl.org/dc/terms/" xmlns:dwc="http://rs.tdwg.org/dwc/terms/" xmlns:obo="http://purl.obolibrary.org/obo/" xmlns:ps="http://purl.org/phenoscape/vocab.owl#" xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="0.9" xsi:schemaLocation="http://www.nexml.org/2009 http://www.nexml.org/2009/nexml.xsd http://www.bioontologies.org/obd/schema/pheno http://purl.org/phenoscape/phenoxml.xsd">
      {
        <meta xsi:type="ResourceMeta" rel="dc:source" href={ study.toString }>
          <meta xsi:type="LiteralMeta" property="dc:bibliographicCitation">
            { model.getProperty(ResourceFactory.createResource(study.toString), ResourceFactory.createProperty(dcBibliographicCitation.getIRI.toString)).getString }
          </meta>
          <meta xsi:type="LiteralMeta" property="rdfs:label" content={ label(study.toString) }/>
        </meta>
      }
      <otus id={ otusID }>
        {
          for {
            otu <- model.listObjectsOfProperty(has_TU)
            taxon = model.listObjectsOfProperty(otu.asResource, has_external_reference).next.asResource
            label = model.listObjectsOfProperty(taxon, rdfsLabel).next.asLiteral.getLexicalForm
            otuID = otuToID(otu.asResource.getURI)
          } yield <otu id={ otuID } about={ s"#$otuID" } label={ label }>
                    <meta xsi:type="ResourceMeta" rel="dwc:taxonID" href={ taxon.getURI }/>
                  </otu>
        }
      </otus>
      <characters id={ s"characters_${UUID.randomUUID.toString}" } xsi:type="StandardCells" otus={ otusID }>
        <format>
          {
            for {
              (character, idSuffix) <- characterToIDSuffix
            } yield <states id={ s"states_$idSuffix" }>
                      {
                        for {
                          stateGroup <- neededStateGroups(character)
                          stateGroupID = stateGroupIDs(stateGroup)
                        } yield if (stateGroup.size < 2)
                          <state id={ stateGroupID } about={ s"#$stateGroupID" } label={ label(stateGroup.head) } symbol={ symbol(stateGroup) }>
                            {
                              val phenotypeURI = model.getProperty(ResourceFactory.createResource(stateGroup.head), describes_phenotype).getResource.getURI
                              // FIXME need to add labels to phenotypes in KB, then move this inside phenotype meta
                              //<meta xsi:type="LiteralMeta" property="rdfs:label" content={ label(phenotypeURI) }/>
                              <meta xsi:type="ResourceMeta" rel="ps:describes_phenotype" href={ phenotypeURI }>
                              </meta>
                            }
                          </state>
                        else
                          <polymorphic_state_set id={ stateGroupID } symbol={ symbol(stateGroup) }>
                            {
                              for {
                                member <- stateGroup
                              } yield <member state={ stateGroupIDs(Set(member)) }/>
                            }
                          </polymorphic_state_set>
                      }
                    </states>
          }
          {
            for {
              character <- orderedCharacters
            } yield <char id={ s"character_${characterToIDSuffix(character)}" } label={ label(character) } states={ s"states_${characterToIDSuffix(character)}" }/>
          }
        </format>
        <matrix>
          {
            for {
              otu <- model.listObjectsOfProperty(has_TU)
              otuURI = otu.asResource.getURI
            } yield <row id={ s"row_${otuToID(otuURI)}" } otu={ otuToID(otuURI) }>
                      {
                        for {
                          cell <- model.listSubjectsWithProperty(belongs_to_TU, otu)
                        } yield <cell char={ s"character_${characterToIDSuffix(model.getProperty(cell, belongs_to_character).getResource.getURI)}" } state={ stateGroupIDs(model.listObjectsOfProperty(cell, has_state).map(_.asResource.getURI).toSet) }/>
                      }
                    </row>
          }
        </matrix>
      </characters>
    </nexml>
  }

  private implicit def owlEntityToJenaProperty(prop: OWLEntity): Property = ResourceFactory.createProperty(prop.getIRI.toString)

  private def queryToTerm(result: QuerySolution): MinimalTerm =
    MinimalTerm(IRI.create(result.getResource("study").getURI),
      result.getLiteral("study_label").getLexicalForm)

}