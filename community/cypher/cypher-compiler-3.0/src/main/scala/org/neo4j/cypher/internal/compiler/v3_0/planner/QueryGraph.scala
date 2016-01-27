/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v3_0.planner

import org.neo4j.cypher.internal.compiler.v3_0.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.perty._

import scala.annotation.tailrec
import scala.collection.{GenTraversableOnce, mutable}

trait Read {
  def readsNodes: Boolean
  def readsRelationships: Boolean
  def nodeIds: Set[IdName]
  def relationships: Set[PatternRelationship]
  def labelsOn(x: IdName): Set[LabelName]
  def typesOn(x: IdName): Set[RelTypeName]
  def propertiesOn(x: IdName): Set[PropertyKeyName]
  def readsProperties: Set[PropertyKeyName]
}

trait Update {

  def overlaps(read: Read): Boolean = nonEmpty && (nodeOverlaps(read) || relOverlaps(read))

  private def nodeOverlaps(read: Read): Boolean = {
    val ids = read.nodeIds
    read.readsNodes && ids.exists { nodeId =>
      val readLabels = read.labelsOn(nodeId)
      val readProps = read.propertiesOn(nodeId)

      val a = {
        if (read.relationships.isEmpty || createsRelationships) {

          val readsNoLabels = readLabels.isEmpty
          val readsNoProps = readProps.isEmpty
          readsNoLabels && readsNoProps && createsNodes
        } else
          false // Looking for rels, but not creating any
      }

      val b = {
        val updatedLabels = addedLabelsNotOn(nodeId) ++ removedLabelsNotOn(nodeId)
        readLabels containsAnyOf updatedLabels
      }

      val c = {
        val updatedProperties = updatesNodePropertiesNotOn(nodeId)
        readProps exists updatedProperties.overlaps
      }

      a || b || c || deletes(nodeId)
    }
  }

  private def relOverlaps(read: Read): Boolean = {
    updatesRelationships && read.readsRelationships && read.relationships.exists { rel =>
      val readTypes = rel.types.toSet
      val readProps = read.propertiesOn(rel.name)
      val updatedProperties = if (rel.dir == SemanticDirection.BOTH)
        allRelationshipPropertyUpdates
      else
        relationshipPropertyUpdatesNotOn(rel.name)
      val createdTypes = relTypesCreated

      val a = {
        val readsNoTypes = readTypes.isEmpty
        val readsNoProps = readProps.isEmpty
        readsNoTypes && readsNoProps && createsRelationships
      }

      val b = {
        readTypes.nonEmpty && (readTypes containsAnyOf createdTypes)
      }

      val c = {
        readProps.nonEmpty && (readProps exists updatedProperties.overlaps)
      }

      val d = {
        deletes(rel.name) && rel.dir == SemanticDirection.BOTH
      }

      val e = readTypes.isEmpty && c
      val g = readProps.isEmpty && b
      a || e || g || d || (b && c)
    }
  }

  def createsNodes: Boolean
  def createsRelationships: Boolean
  def updatesRelationships: Boolean
  def deletes(name: IdName): Boolean
  def isEmpty: Boolean
  def nonEmpty: Boolean = !isEmpty

  def addedLabelsNotOn(id: IdName): Set[LabelName]
  def removedLabelsNotOn(id: IdName): Set[LabelName]

  def updatesNodePropertiesNotOn(id: IdName): CreatesPropertyKeys
  def relationshipPropertyUpdatesNotOn(id: IdName): CreatesPropertyKeys
  def allRelationshipPropertyUpdates: CreatesPropertyKeys
  def containsDeletes: Boolean

  def relTypesCreated: Set[RelTypeName]

  implicit class apa[T](my: Set[T]) {
    def containsAnyOf(other:Set[T]) = (my intersect other).nonEmpty
  }
}

case class ReadView(qg: QueryGraph) extends Read {
  override def readsNodes = qg.patternNodes.nonEmpty
  override def readsRelationships = qg.patternRelationships.nonEmpty

  override def nodeIds = qg.patternNodes
  override def labelsOn(x: IdName): Set[LabelName] = qg.selections
    .labelPredicates.getOrElse(x, Set.empty)
    .flatMap(_.labels)

  override def relationships = qg.patternRelationships

  override def typesOn(x: IdName): Set[RelTypeName] = qg.patternRelationships.collect {
    case rel if rel.name == x => rel.types
  }.flatten

  override def readsProperties: Set[PropertyKeyName] =
    qg.allKnownNodeProperties.map(_.propertyKey)

  override def propertiesOn(x: IdName): Set[PropertyKeyName] =
    qg.knownProperties(x).map(_.propertyKey)
}

case class UpdateView(mutatingPatterns: Seq[MutatingPattern]) extends Update {

  override def deletes(name: IdName) = {

    @tailrec
    def matches(step: PathStep): Boolean = step match {
      case NodePathStep(Variable(id), next) => id == name.name || matches(next)
      case SingleRelationshipPathStep(Variable(id), _, next) => id == name.name || matches(next)
      case MultiRelationshipPathStep(Variable(id), _, next) => id == name.name || matches(next)
      case NilPathStep => false
    }

    mutatingPatterns.exists {
      case DeleteExpressionPattern(Variable(id), _) if id == name.name => true
      case DeleteExpressionPattern(PathExpression(pathStep), _) if matches(pathStep) => true
      case _ => false
    }
  }

  override def createsRelationships = mutatingPatterns.exists(_.isInstanceOf[CreateRelationshipPattern])

  override def updatesRelationships = mutatingPatterns.exists {
    case _: CreateRelationshipPattern => true
    case _: SetRelationshipPropertiesFromMapPattern => true
    case _: SetRelationshipPropertyPattern => true
    case _: DeleteExpressionPattern => true
    case _ => false
  }

  override def isEmpty = mutatingPatterns.isEmpty

  override def addedLabelsNotOn(id: IdName): Set[LabelName] = (mutatingPatterns collect {
    case x: SetLabelPattern if x.idName != id => x.labels
    case x: CreateNodePattern => x.labels

  }).flatten.toSet

  override def removedLabelsNotOn(id: IdName): Set[LabelName] = (mutatingPatterns collect {
    case x: RemoveLabelPattern if x.idName != id => x.labels
  }).flatten.toSet

  private def updatesRelationshipProperties = {
    @tailrec
    def toRelPropertyPattern(patterns: Seq[MutatingPattern], acc: CreatesPropertyKeys): CreatesPropertyKeys = {

      def extractPropertyKey(patterns: Seq[SetMutatingPattern]): CreatesPropertyKeys = patterns.collect {
        case SetRelationshipPropertyPattern(_, key, _) => CreatesKnownPropertyKeys(key)
        case SetRelationshipPropertiesFromMapPattern(_, expression, _) => CreatesPropertyKeys(expression)
      }.foldLeft[CreatesPropertyKeys](CreatesNoPropertyKeys)(_ + _)

      patterns match {
        case Nil => acc
        case SetRelationshipPropertiesFromMapPattern(_, expression, _) :: tl => CreatesPropertyKeys(expression)
        case SetRelationshipPropertyPattern(_, key, _) :: tl => toRelPropertyPattern(tl, acc + CreatesKnownPropertyKeys(key))
        case MergeNodePattern(_, _, onCreate, onMatch) :: tl =>
          toRelPropertyPattern(tl, acc + extractPropertyKey(onCreate) + extractPropertyKey(onMatch))
        case MergeRelationshipPattern(_, _, _, onCreate, onMatch) :: tl =>
          toRelPropertyPattern(tl, acc + extractPropertyKey(onCreate) + extractPropertyKey(onMatch))

        case hd :: tl => toRelPropertyPattern(tl, acc)
      }
    }

    toRelPropertyPattern(mutatingPatterns, CreatesNoPropertyKeys)
  }

  override def containsDeletes = mutatingPatterns.exists(_.isInstanceOf[DeleteExpressionPattern])

  override def createsNodes = mutatingPatterns.exists(_.isInstanceOf[CreateNodePattern])

  override def updatesNodePropertiesNotOn(id: IdName): CreatesPropertyKeys = mutatingPatterns.foldLeft[CreatesPropertyKeys](CreatesNoPropertyKeys) {
    case (acc, c: SetNodePropertyPattern) if c.idName != id => acc + CreatesKnownPropertyKeys(Set(c.propertyKey))
    case (acc, c: SetNodePropertiesFromMapPattern) if c.idName != id => acc + CreatesPropertyKeys(c.expression)
    case (acc, CreateNodePattern(_, _, Some(properties))) => acc + CreatesPropertyKeys(properties)
    case (acc, _) => acc
  }

  override def relationshipPropertyUpdatesNotOn(id: IdName): CreatesPropertyKeys = collectPropertyUpdates(_ != id)

  override def allRelationshipPropertyUpdates: CreatesPropertyKeys = collectPropertyUpdates(_ => true)

  private def collectPropertyUpdates(f: (IdName => Boolean)) = mutatingPatterns.foldLeft[CreatesPropertyKeys](CreatesNoPropertyKeys) {
    case (acc, c: SetRelationshipPropertyPattern) if f(c.idName) => acc + CreatesKnownPropertyKeys(Set(c.propertyKey))
    case (acc, c: SetRelationshipPropertiesFromMapPattern) if f (c.idName) => acc + CreatesPropertyKeys(c.expression)
    case (acc, CreateRelationshipPattern(_, _, _, _, Some(props), _)) => acc + CreatesPropertyKeys(props)
    case (acc, _) => acc
  }

  override def relTypesCreated: Set[RelTypeName] = mutatingPatterns.collect {
    case p: CreateRelationshipPattern => p.relType
  }.toSet
}

case class QueryGraph(patternRelationships: Set[PatternRelationship] = Set.empty,
                      patternNodes: Set[IdName] = Set.empty,
                      argumentIds: Set[IdName] = Set.empty,
                      selections: Selections = Selections(),
                      optionalMatches: Seq[QueryGraph] = Seq.empty,
                      hints: Set[Hint] = Set.empty,
                      shortestPathPatterns: Set[ShortestPathPattern] = Set.empty,
                      mutatingPatterns: Seq[MutatingPattern] = Seq.empty)
  extends UpdateGraph with PageDocFormatting {
  self =>
  // TODO: Add assertions to make sure invalid QGs are rejected, such as mixing MERGE with other clauses

  def reads: Read = {
    val queryGraph = if (containsMerge)
      mutatingPatterns.head.asInstanceOf[MergePattern].matchGraph
    else
      optionalMatches.foldLeft(this) {
        case (acc, qg) => acc ++ qg
      }
    ReadView(queryGraph)
  }

  def updates: Update = {
    val updateActions = if (containsMerge) {
      mutatingPatterns.collect {
        case x: MergeNodePattern => Seq(x.createNodePattern) ++ x.onCreate ++ x.onMatch
        case x: MergeRelationshipPattern => x.createNodePatterns ++ x.createRelPatterns ++ x.onCreate ++ x.onMatch
      }.flatten
    } else mutatingPatterns

    UpdateView(updateActions)
  }

  def size = patternRelationships.size

  def isEmpty: Boolean = this == QueryGraph.empty

  def nonEmpty: Boolean = !isEmpty

  def mapSelections(f: Selections => Selections): QueryGraph =
    copy(selections = f(selections), optionalMatches = optionalMatches.map(_.mapSelections(f)))

  def addPatternNodes(nodes: IdName*): QueryGraph = copy(patternNodes = patternNodes ++ nodes)

  def addPatternRelationship(rel: PatternRelationship): QueryGraph =
    copy(
      patternNodes = patternNodes + rel.nodes._1 + rel.nodes._2,
      patternRelationships = patternRelationships + rel
    )

  def addPatternRelationships(rels: Seq[PatternRelationship]) =
    rels.foldLeft[QueryGraph](this)((qg, rel) => qg.addPatternRelationship(rel))

  def addShortestPath(shortestPath: ShortestPathPattern): QueryGraph = {
    val rel = shortestPath.rel
    copy (
      patternNodes = patternNodes + rel.nodes._1 + rel.nodes._2,
      shortestPathPatterns = shortestPathPatterns + shortestPath
    )
  }

  /*
  Includes not only pattern nodes in the read part of the query graph, but also pattern nodes from CREATE and MERGE
   */
  def allPatternNodes: Set[IdName] =
    patternNodes ++
    optionalMatches.flatMap(_.allPatternNodes) ++
    createNodePatterns.map(_.nodeName) ++
    mergeNodePatterns.map(_.createNodePattern.nodeName) ++
    mergeRelationshipPatterns.flatMap(_.createNodePatterns.map(_.nodeName))

  def allPatternNodesRead: Set[IdName] =
    patternNodes ++ optionalMatches.flatMap(_.allPatternNodesRead)

  def addShortestPaths(shortestPaths: ShortestPathPattern*): QueryGraph = shortestPaths.foldLeft(this)((qg, p) => qg.addShortestPath(p))
  def addArgumentId(newId: IdName): QueryGraph = copy(argumentIds = argumentIds + newId)
  def addArgumentIds(newIds: Seq[IdName]): QueryGraph = copy(argumentIds = argumentIds ++ newIds)
  def addSelections(selections: Selections): QueryGraph =
    copy(selections = Selections(selections.predicates ++ this.selections.predicates))

  def addPredicates(predicates: Expression*): QueryGraph = {
    val newSelections = Selections(predicates.flatMap(_.asPredicates).toSet)
    copy(selections = selections ++ newSelections)
  }

  def addHints(addedHints: GenTraversableOnce[Hint]): QueryGraph = copy(hints = hints ++ addedHints)

  def withoutHints(hintsToIgnore: GenTraversableOnce[Hint]): QueryGraph = copy(hints = hints -- hintsToIgnore)

  def withoutArguments(): QueryGraph = withArgumentIds(Set.empty)
  def withArgumentIds(newArgumentIds: Set[IdName]): QueryGraph =
    copy(argumentIds = newArgumentIds)

  def withAddedOptionalMatch(optionalMatch: QueryGraph): QueryGraph = {
    val argumentIds = allCoveredIds intersect optionalMatch.allCoveredIds
    copy(optionalMatches = optionalMatches :+ optionalMatch.addArgumentIds(argumentIds.toSeq))
  }

  def withOptionalMatches(optionalMatches: Seq[QueryGraph]): QueryGraph = {
    copy(optionalMatches = optionalMatches)
  }

  def withSelections(selections: Selections): QueryGraph = copy(selections = selections)

  def knownProperties(idName: IdName): Set[Property] =
    selections.propertyPredicatesForSet.getOrElse(idName, Set.empty)

  private def knownLabelsOnNode(node: IdName): Set[LabelName] =
    selections
      .labelPredicates.getOrElse(node, Set.empty)
      .flatMap(_.labels)

  def allKnownLabelsOnNode(node: IdName): Set[LabelName] =
    knownLabelsOnNode(node) ++ optionalMatches.flatMap(_.allKnownLabelsOnNode(node))

  def allKnownPropertiesOnIdentifier(idName: IdName): Set[Property] =
    knownProperties(idName) ++ optionalMatches.flatMap(_.allKnownPropertiesOnIdentifier(idName))

  def allKnownNodeProperties: Set[Property] = {
    val matchedNodes = patternNodes ++ patternRelationships.flatMap(r => Set(r.nodes._1, r.nodes._2))
    matchedNodes.flatMap(knownProperties) ++ optionalMatches.flatMap(_.allKnownNodeProperties)
  }

  def allKnownRelProperties: Set[Property] =
    patternRelationships.map(_.name).flatMap(knownProperties) ++ optionalMatches.flatMap(_.allKnownRelProperties)


  def findRelationshipsEndingOn(id: IdName): Set[PatternRelationship] =
    patternRelationships.filter { r => r.left == id || r.right == id }

  def allPatternRelationships: Set[PatternRelationship] =
    patternRelationships ++ optionalMatches.flatMap(_.allPatternRelationships) ++
    // Recursively add relationships from the merge-read-part
    mergeNodePatterns.flatMap(_.matchGraph.allPatternRelationships) ++
    mergeRelationshipPatterns.flatMap(_.matchGraph.allPatternRelationships)


  def coveredIds: Set[IdName] = {
    val patternIds = QueryGraph.coveredIdsForPatterns(patternNodes, patternRelationships)
    patternIds ++ argumentIds ++ selections.predicates.flatMap(_.dependencies)
  }

  def allCoveredIds: Set[IdName] = {
    val otherSymbols = optionalMatches.flatMap(_.allCoveredIds) ++ mutatingPatterns.flatMap(_.coveredIds)
    coveredIds ++ otherSymbols
  }

  val allHints: Set[Hint] =
    if (optionalMatches.isEmpty) hints else hints ++ optionalMatches.flatMap(_.allHints)

  def numHints = allHints.size

  def ++(other: QueryGraph): QueryGraph =
    QueryGraph(
      selections = selections ++ other.selections,
      patternNodes = patternNodes ++ other.patternNodes,
      patternRelationships = patternRelationships ++ other.patternRelationships,
      optionalMatches = optionalMatches ++ other.optionalMatches,
      argumentIds = argumentIds ++ other.argumentIds,
      hints = hints ++ other.hints,
      shortestPathPatterns = shortestPathPatterns ++ other.shortestPathPatterns,
      mutatingPatterns = mutatingPatterns ++ other.mutatingPatterns
    )

  def isCoveredBy(other: QueryGraph): Boolean = {
    patternNodes.subsetOf(other.patternNodes) &&
      patternRelationships.subsetOf(other.patternRelationships) &&
      argumentIds.subsetOf(other.argumentIds) &&
      optionalMatches.toSet.subsetOf(other.optionalMatches.toSet) &&
      selections.predicates.subsetOf(other.selections.predicates) &&
      shortestPathPatterns.subsetOf(other.shortestPathPatterns)
  }

  def covers(other: QueryGraph): Boolean = other.isCoveredBy(this)

  def hasOptionalPatterns = optionalMatches.nonEmpty

  def patternNodeLabels: Map[IdName, Set[LabelName]] =
    patternNodes.collect { case node: IdName => node -> selections.labelsOnNode(node) }.toMap

  /**
   * Returns the connected patterns of this query graph where each connected pattern is represented by a QG.
   * Does not include optional matches, shortest paths or predicates that have dependencies across multiple of the
   * connected query graphs.
   */
  def connectedComponents: Seq[QueryGraph] = {
    val visited = mutable.Set.empty[IdName]
    patternNodes.toSeq.collect {
      case patternNode if !visited(patternNode) =>
        val qg = connectedComponentFor(patternNode, visited)
        val coveredIds = qg.coveredIds
        val predicates = selections.predicates.filter(_.dependencies.subsetOf(coveredIds))
        val arguments = argumentIds
        val filteredHints = hints.filter(h => h.variables.forall(variable => coveredIds.contains(IdName(variable.name))))
        val shortestPaths = shortestPathPatterns.filter {
          p => coveredIds.contains(p.rel.nodes._1) && coveredIds.contains(p.rel.nodes._2)
        }
        qg.
          withSelections(Selections(predicates)).
          withArgumentIds(arguments).
          addHints(filteredHints).
          addShortestPaths(shortestPaths.toSeq: _*)
    }
  }

  def withoutPatternRelationships(patterns: Set[PatternRelationship]): QueryGraph =
    copy(patternRelationships = patternRelationships -- patterns)

  def withoutPatternNode(patterns: IdName): QueryGraph =
    copy(patternNodes = patternNodes - patterns)

  def joinHints: Set[UsingJoinHint] =
    hints.collect { case hint: UsingJoinHint => hint }

  private def connectedComponentFor(startNode: IdName, visited: mutable.Set[IdName]): QueryGraph = {
    val queue = mutable.Queue(startNode)
    var qg = QueryGraph.empty
    while (queue.nonEmpty) {
      val node = queue.dequeue()
      if (!visited(node)) {
        visited += node

        val filteredPatterns = patternRelationships.filter { rel =>
          rel.coveredIds.contains(node) && !qg.patternRelationships.contains(rel)
        }

        queue.enqueue(filteredPatterns.toSeq.map(_.otherSide(node)): _*)

        qg = qg
          .addPatternNodes(node)
          .addPatternRelationships(filteredPatterns.toSeq)

        val alreadyHaveArguments = qg.argumentIds.nonEmpty

        if (!alreadyHaveArguments && (relationshipPullsInArguments(qg.coveredIds) || predicatePullsInArguments(node))) {
          qg = qg.withArgumentIds(argumentIds)
          val nodesSolvedByArguments = patternNodes intersect qg.argumentIds
          queue.enqueue(nodesSolvedByArguments.toSeq: _*)
        }
      }
    }
    qg
  }

  private def relationshipPullsInArguments(coveredIds: Set[IdName]) = (argumentIds intersect coveredIds).nonEmpty

  private def predicatePullsInArguments(node: IdName) = selections.flatPredicates.exists {
    case p =>
      val deps = p.dependencies.map(IdName.fromVariable)
      deps(node) && (deps intersect argumentIds).nonEmpty
  }

  def containsReads: Boolean = {
    (patternNodes -- argumentIds).nonEmpty ||
    patternRelationships.nonEmpty ||
    selections.nonEmpty ||
    shortestPathPatterns.nonEmpty ||
    optionalMatches.nonEmpty ||
    containsMerge
  }

  def writeOnly = !containsReads && containsUpdates

  def addMutatingPatterns(patterns: MutatingPattern*): QueryGraph =
    copy(mutatingPatterns = mutatingPatterns ++ patterns)

  // This is here to stop usage of copy from the outside
  private def copy(patternRelationships: Set[PatternRelationship] = patternRelationships,
                   patternNodes: Set[IdName] = patternNodes,
                   argumentIds: Set[IdName] = argumentIds,
                   selections: Selections = selections,
                   optionalMatches: Seq[QueryGraph] = optionalMatches,
                   hints: Set[Hint] = hints,
                   shortestPathPatterns: Set[ShortestPathPattern] = shortestPathPatterns,
                   mutatingPatterns: Seq[MutatingPattern] = mutatingPatterns) =
  QueryGraph(patternRelationships, patternNodes, argumentIds, selections, optionalMatches, hints, shortestPathPatterns, mutatingPatterns)
}

object QueryGraph {
  val empty = QueryGraph()

  def coveredIdsForPatterns(patternNodeIds: Set[IdName], patternRels: Set[PatternRelationship]) = {
    val patternRelIds = patternRels.flatMap(_.coveredIds)
    patternNodeIds ++ patternRelIds
  }

  implicit object byCoveredIds extends Ordering[QueryGraph] {

    import scala.math.Ordering.Implicits

    def compare(x: QueryGraph, y: QueryGraph): Int = {
      val xs = x.coveredIds.toSeq.sorted(IdName.byName)
      val ys = y.coveredIds.toSeq.sorted(IdName.byName)
      Implicits.seqDerivedOrdering[Seq, IdName](IdName.byName).compare(xs, ys)
    }
  }

  val useOldUpdateGraphOverlapsMethod = false
}
