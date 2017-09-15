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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.apa.v3_4.{ASTNode, InputPosition}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{SemanticAnalysis, SemanticCheckable}
import org.neo4j.cypher.internal.frontend.v3_4.symbols._

sealed trait RemoveItem extends ASTNode with SemanticCheckable

case class RemoveLabelItem(variable: Variable, labels: Seq[LabelName])(val position: InputPosition) extends RemoveItem {
  def semanticCheck =
    SemanticAnalysis.semanticCheck(Expression.SemanticContext.Simple, variable) chain
    SemanticAnalysis.expectType(CTNode.covariant, variable)
}

case class RemovePropertyItem(property: Property) extends RemoveItem {
  def position = property.position

  def semanticCheck = SemanticAnalysis.semanticCheck(Expression.SemanticContext.Simple, property)
}
