/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.ir.v3_4

import org.bitbucket.inkytonik.kiama.output.PrettyPrinter
import org.neo4j.cypher.internal.frontend.v3_4.ast.{AscSortItem, DescSortItem, SortItem}
import org.neo4j.cypher.internal.frontend.v3_4.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, Namespace, SemanticDirection}

object IRPrettyPrinter extends PrettyPrinter {
  val stringifier = ExpressionStringifier(e => e.asCanonicalStringVal)
  val PRINT_QUERY_TEXT = true
  val PRINT_LOGICAL_PLAN = true
  val PRINT_REWRITTEN_LOGICAL_PLAN = true
  val PRINT_PIPELINE_INFO = true
  val PRINT_FAILURE_STACK_TRACE = true

  def show(unionQuery: UnionQuery): Doc = {
    val queries = unionQuery.queries.map(showAll)
    nest(group(sep(queries.toList)))
  }

  def showAll(pq: PlannerQuery): Doc = {
    val docs = pq.allPlannerQueries.toList.map(showSingle)
    text("PlannerQuery") <> brackets(

      nest(linebreak <> vsep(docs)) <>
        linebreak
    )
  }

  def show(horizon: QueryHorizon): Doc = horizon match {
    case UnwindProjection(variable, e) =>
      "Unwind" <+> show(e) <+> "AS" <+> show(variable)

    case PassthroughAllHorizon() =>
      emptyDoc

    case LoadCSVProjection(variable, url, format, fields) =>
      ???

    case RegularQueryProjection(projections, shuffle) =>
      "Projection" <+> braces(show(projections)) <>
      show(shuffle)

    case DistinctQueryProjection(projections, shuffle) =>
      "Distinct Projection" <+> braces(show(projections)) <>
      show(shuffle)

    case AggregatingQueryProjection(grouping, aggregation, shuffle) =>
      "Aggregate" <+> braces(show(aggregation)) <+> "Group By" <+> braces(show(grouping)) <>
        show(shuffle)
  }

  private def show(projections: Map[String, Expression]): Doc = {
    val docs = projections.toList map {
      case (k, e) => show(e) <+> "AS" <+> k
    }
    " " <> nest(hsep(docs, comma)) <> " "
  }

  def show(sortItem: SortItem): Doc = sortItem match {
    case AscSortItem(e) => show(e) <+> "ASC"
    case DescSortItem(e) => show(e) <+> "DESC"
  }

  def show(shuffle: QueryShuffle): Doc = {
    val limit = shuffle.limit.map(e => text("LIMIT") <+> show(e))
    val skip = shuffle.skip.map(e => text("SKIP") <+> show(e))
    val orderBy = groupElements("ORDER BY", shuffle.sortItems, show(_: SortItem))

    val docs = toSeq(limit, skip, orderBy)
    ifTrue(docs.nonEmpty, linebreak <> "- QueryShuffle:" <+> hsep(docs))
  }

  def showSingle(pq: PlannerQuery): Doc = {
    "- " <> show(pq.queryGraph) <> linebreak <>
      "- " <> show(pq.horizon)
  }

  def show(id: IdName): Doc = enclose("'", id.name, "'")

  def show(e: Expression): Doc = {
    text(stringifier.apply(e))
  }

  def show(mutatingPatterns: Seq[MutatingPattern]): Doc = {
    val mutatingDocs: Seq[Doc] = mutatingPatterns map {
      case CreateNodePattern(IdName(name), labels, maybeProps) =>
        text("apa")
    }
    vsep(mutatingDocs.toList)
  }

  def show(queryGraph: QueryGraph): Doc = {
    val nodes = groupElements("Nodes:", queryGraph.patternNodes, show(_: IdName))
    val arguments = groupElements("Arguments:", queryGraph.argumentIds, show(_: IdName))
    val edges = groupElements("Edges:", queryGraph.patternRelationships, show(_: PatternRelationship))
    val predicates = groupElements("Predicates:", queryGraph.selections.flatPredicates, show(_: Expression))
    val optionals = groupElements("Optional matches:", queryGraph.optionalMatches, line <> show(_: QueryGraph), container = d => nest(d))
    val shortestPaths = groupElements("Shortest paths:", queryGraph.shortestPathPatterns, (p: ShortestPathPattern) => text(p.toString))
    val updates = if (queryGraph.mutatingPatterns.isEmpty) None else Some(show(queryGraph.mutatingPatterns))
    val content = line <> ssep(
      toSeq(arguments, nodes, edges, predicates, optionals, shortestPaths, updates),
      line
    )

    "QueryGraph" <+> braces(group(nest(content) <> line))
  }

  private def toSeq(values: Option[Doc]*): collection.immutable.Seq[Doc] = {
    values.flatten.toList
  }

  private def groupElements[T](name: Doc, patternNodes: Traversable[T], f: T => Doc, container: Doc => Doc = identity): Option[Doc] =
    if (patternNodes.isEmpty)
      None
    else
      Some(container(name <+> brackets(hsep(patternNodes.map(f).toList, ","))))

  private def show(p: PatternRelationship): Doc = {
    val (lDoc, rDoc) = p.dir match {
      case SemanticDirection.INCOMING => "<-" -> "-"
      case SemanticDirection.OUTGOING => "-" -> "->"
      case SemanticDirection.BOTH => "-" -> "-"
    }

    val relInfo = {
      val NAME = show(p.name)
      val LENGTH: Doc = p.length match {
        case SimplePatternLength => emptyDoc
        case VarPatternLength(min, None) => s"*$min.."
        case VarPatternLength(min, Some(max)) => s"*$min..$max"
      }
      val TYPES = if (p.types.isEmpty) emptyDoc else
        colon <> folddoc(p.types.toList.map(t => text(t.name)), _ <> "|" <> _)


      group(brackets(NAME <> TYPES <> LENGTH))
    }

    val lNode = parens(show(p.nodes._1))
    val rNode = parens(show(p.nodes._2))
    group(lNode <> lDoc <> relInfo <> rDoc <> rNode)
  }

  private def ifTrue(opt: Boolean, f: Doc) =
    if (opt) f else emptyDoc

  private def hlist(docs: Traversable[Doc], sep: Doc = emptyDoc): Doc = hsep(docs.toList, sep)

  private def listNoSpace(docs: Traversable[Doc], sep: Doc = emptyDoc): Doc = ssep(docs.toList, sep)

  private def fqn(ns: Namespace, name: String) = {
    val elements = ns.parts.map(string) :+ string(name)
    folddoc(elements, _ <> dot <> _)
  }

  private def maybe[T](v: Option[T], f: T => Doc): Doc =
    v.map(f(_)).getOrElse(emptyDoc)

  private def vlist(docs: Traversable[Doc], sep: Doc = emptyDoc): Doc = vsep(docs.toList, sep)
}
