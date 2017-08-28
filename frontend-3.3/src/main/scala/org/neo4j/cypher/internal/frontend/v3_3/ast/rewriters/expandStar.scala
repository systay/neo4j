/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.{Rewriter, SemanticState, bottomUp}

case class expandStar(state: SemanticState) extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case clause@With(_, values, graphs, _, _, _, _)
      if values.includeExisting || graphs.exists(_.star) =>
      val newReturnItems = if (values.includeExisting) returnItems(clause, values.items) else values
      val newGraphItems = graphs match {
        case Some(GraphReturnItems(true, graphItems)) => graphReturnItems(clause, graphItems)
        case _ => graphs
      }
      clause.copy(returnItems = newReturnItems, graphReturnItems = newGraphItems)(clause.position)

    case clause: PragmaWithout =>
      With(
        distinct = false,
        returnItems = returnItems(clause, Seq.empty, clause.excludedNames),
        graphReturnItems = graphReturnItems(clause, List.empty, clause.excludedNames),
        orderBy = None, skip = None, limit = None, where = None)(clause.position)

    case clause@Return(_, values, graphs, _, _, _, excludedNames)
      if values.includeExisting || graphs.exists(_.star) =>
      val newReturnItems = if (values.includeExisting) returnItems(clause, values.items, excludedNames) else values
      val newGraphItems = graphs match {
        case Some(GraphReturnItems(true, graphItems)) => graphReturnItems(clause, graphItems, excludedNames)
        case _ => graphs
      }
      clause.copy(returnItems = newReturnItems, graphReturnItems = newGraphItems, excludedNames = Set.empty)(clause.position)

    case expandedAstNode =>
      expandedAstNode
  }

  private val instance = bottomUp(rewriter, _.isInstanceOf[Expression])

  private def graphReturnItems(clause: Clause, listedItems: List[GraphReturnItem], excludedNames: Set[String] = Set.empty)
  : Option[GraphReturnItems] = {
    val scope = state.scope(clause).getOrElse {
      throw new IllegalStateException(s"${clause.name} should note its Scope in the SemanticState")
    }
    val clausePos = clause.position
    val symbolNames = scope.selectSymbolNames(_.graph) -- excludedNames
    val newGraphItems = symbolNames.toIndexedSeq.sorted.map { id =>
      val idPos = scope.symbolTable(id).definition.position
      val expr = Variable(id)(idPos)
      val alias = expr.copyId
      ReturnedGraph(
        GraphAs(
          expr,
          Some(alias)
        )(clausePos)
      )(clausePos)
    }.toList ++ listedItems
    if (newGraphItems.isEmpty) None else Some(GraphReturnItems(star = false, newGraphItems)(clausePos))
  }

  private def returnItems(clause: Clause, listedItems: Seq[ReturnItem], excludedNames: Set[String] = Set.empty)
  : ReturnItemsDef = {
    val scope = state.scope(clause).getOrElse {
      throw new IllegalStateException(s"${clause.name} should note its Scope in the SemanticState")
    }

    val clausePos = clause.position
    val symbolNames = scope.selectSymbolNames(! _.graph) -- excludedNames
    val expandedItems = symbolNames.toIndexedSeq.sorted.map { id =>
      val idPos = scope.symbolTable(id).definition.position
      val expr = Variable(id)(idPos)
      val alias = expr.copyId
      AliasedReturnItem(expr, alias)(clausePos)
    }

    val newItems = expandedItems ++ listedItems
    if (newItems.isEmpty)
      EmptyReturnItems(fromRewriting = true)(clausePos)
    else
      ReturnItems(includeExisting = false, newItems)(clausePos)
  }
}
