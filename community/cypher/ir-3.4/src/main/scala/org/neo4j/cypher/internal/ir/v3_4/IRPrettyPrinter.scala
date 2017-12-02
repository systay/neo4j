/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ir.v3_4

import org.bitbucket.inkytonik.kiama.output.PrettyPrinter
import org.neo4j.cypher.internal.v3_4.expressions.{Namespace, SemanticDirection}

object IRPrettyPrinter extends PrettyPrinter {
  val PRINT_QUERY_TEXT = true
  val PRINT_LOGICAL_PLAN = true
  val PRINT_REWRITTEN_LOGICAL_PLAN = true
  val PRINT_PIPELINE_INFO = true
  val PRINT_FAILURE_STACK_TRACE = true

  def show(unionQuery: UnionQuery): Doc = {
    val queries = unionQuery.queries.map(show)
    nest(group(sep(queries.toList)))
  }

  def show(pq: PlannerQuery): Doc = ???

  def show(id: IdName): Doc = enclose("'", id.name, "'")

  def show(queryGraph: QueryGraph): Doc = {
    val nodes = groupElements("Nodes:", queryGraph.patternNodes, show(_: IdName))
    val arguments = groupElements("Arguments:", queryGraph.argumentIds, show(_: IdName))
    val edges = groupElements("Edges:", queryGraph.patternRelationships, show(_: PatternRelationship))
    val content = line <> ssep(
      toSeq(arguments, nodes, edges),
      line
    )

    "QueryGraph" <+> braces(group(nest(content) <> line))
  }

  private def toSeq(values: Option[Doc]*): collection.immutable.Seq[Doc] = {
    collection.immutable.Seq(values.flatten:_*)
  }

  private def groupElements[T](name: String, patternNodes: Traversable[T], f: T => Doc): Option[Doc] =
    if (patternNodes.isEmpty)
      None
    else
      Some(name <+> brackets(hsep(patternNodes.map(f).toList, ",")))

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
        colon <> folddoc(p.types.toList.map(t => text("")), _ <> "|" <> _)


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

  //  protected def printPlanInfo(lpState: LogicalPlanState) = {
  //    println(s"\n========================================================================")
  //    if (PRINT_QUERY_TEXT)
  //      println(s"\u001b[32m[QUERY]\n\n${lpState.queryText}") // Green
  //    if (PRINT_LOGICAL_PLAN) {
  //      println(s"\n\u001b[35m[LOGICAL PLAN]\n") // Magenta
  //      prettyPrintLogicalPlan(lpState.logicalPlan)
  //    }
  //    println("\u001b[30m")
  //  }
  //
  //  protected def printRewrittenPlanInfo(logicalPlan: LogicalPlan) = {
  //    if (PRINT_REWRITTEN_LOGICAL_PLAN) {
  //      println(s"\n\u001b[35m[REWRITTEN LOGICAL PLAN]\n") // Magenta
  //      prettyPrintLogicalPlan(logicalPlan)
  //    }
  //    println("\u001b[30m")
  //  }
  //
  //  protected def printPipeInfo(slotConfigurations: Map[LogicalPlanId, SlotConfiguration], pipeInfo: PipeInfo) = {
  //    if (PRINT_PIPELINE_INFO) {
  //      println(s"\n\u001b[36m[SLOT CONFIGURATIONS]\n") // Cyan
  //      prettyPrintPipelines(slotConfigurations)
  //      println(s"\n\u001b[34m[PIPE INFO]\n") // Blue
  //      prettyPrintPipeInfo(pipeInfo)
  //    }
  //    println("\u001b[30m")
  //  }
  //
  //  protected def printFailureStackTrace(e: CypherException) = {
  //    if (PRINT_FAILURE_STACK_TRACE) {
  //      println("------------------------------------------------")
  //      println("<<< Slotted failed because:\u001b[31m") // Red
  //      e.printStackTrace(System.out)
  //      println("\u001b[30m>>>")
  //      println("------------------------------------------------")
  //    }
  //  }

  //  private def prettyPrintLogicalPlan(plan: LogicalPlan): Unit = {
  //    val planAnsiPre = "\u001b[1m\u001b[35m" // Bold on + magenta
  //    val planAnsiPost = "\u001b[21m\u001b[35m" // Restore to bold off + magenta
  //    def prettyPlanName(plan: LogicalPlan) = s"$planAnsiPre${plan.productPrefix}$planAnsiPost"
  //    def prettyId(id: LogicalPlanId) = s"\u001b[4m\u001b[35m${id}\u001b[24m\u001b[35m" // Underlined + magenta
  //
  //    def show(v: Any): Doc =
  //      link(v.asInstanceOf[AnyRef],
  //        v match {
  //          case id: LogicalPlanId =>
  //            text(prettyId(id))
  //
  //          case plan: LogicalPlan =>
  //            (plan.lhs, plan.rhs) match {
  //              case (None, None) =>
  //                val elements = plan.productIterator.toList
  //                list(plan.assignedId :: elements, prettyPlanName(plan), show)
  //
  //              case (Some(lhs), None) =>
  //                val otherElements: List[Any] = plan.productIterator.toList.filter {
  //                  case e: AnyRef => e ne lhs
  //                  case _ => true
  //                }
  //                list(plan.assignedId :: otherElements, prettyPlanName(plan), show) <>
  //                  line <> show(lhs)
  //
  //              case (Some(lhs), Some(rhs)) =>
  //                val otherElements: List[Any] = plan.productIterator.toList.filter {
  //                  case e: AnyRef => (e ne lhs) && (e ne rhs)
  //                  case _ => true
  //                }
  //                val lhsDoc = "[LHS]" <> line <> nest(show(lhs), 2)
  //                val rhsDoc = s"[RHS of ${plan.getClass.getSimpleName} (${plan.assignedId})]" <> line <> nest(show(rhs), 2)
  //                list(plan.assignedId :: otherElements, prettyPlanName(plan), show) <>
  //                  line <> nest(lhsDoc, 2) <>
  //                  line <> nest(rhsDoc, 2)
  //
  //              case _ =>
  //                throw new InternalException("Invalid logical plan structure")
  //            }
  //
  //          case _ =>
  //            any(v)
  //        }
  //      )
  //
  //    val prettyDoc = pretty(show(plan), w = 120)
  //    println(prettyDoc.layout)
  //  }

  //  protected def prettyPrintPipelines(pipelines: Map[LogicalPlanId, SlotConfiguration]): Unit = {
  //    val transformedPipelines = pipelines.foldLeft(Seq.empty[Any]) {
  //      case (acc, (k: LogicalPlanId, v)) => acc :+ (k.underlying -> v)
  //    }.sortBy { case (k: Int, _) => k }
  //    val prettyDoc = pretty(any(transformedPipelines), w = 120)
  //    println(prettyDoc.layout)
  //  }

  //  protected def prettyPrintPipeInfo(pipeInfo: PipeInfo): Unit = {
  //    val prettyDoc = pretty(any(pipeInfo), w = 120)
  //    println(prettyDoc.layout)
  //  }
}
