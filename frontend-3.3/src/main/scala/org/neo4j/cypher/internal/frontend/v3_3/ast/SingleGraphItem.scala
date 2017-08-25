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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheck, SemanticCheckResult, SemanticCheckable, SemanticChecking}

sealed trait SingleGraphItem extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking {

  def as: Option[Variable]

  def withNewName(newName: Variable): SingleGraphItem

  override def semanticCheck: SemanticCheck = {
    inner chain as.map(_.declareGraph: SemanticCheck).getOrElse(SemanticCheckResult.success)
  }

  protected def inner: SemanticCheck
}

final case class GraphOfItem(of: Pattern, as: Option[Variable])(val position: InputPosition) extends SingleGraphItem {
  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = of.semanticCheck(Pattern.SemanticContext.Create)
}

final case class GraphAtItem(at: GraphUrl, as: Option[Variable])(val position: InputPosition) extends SingleGraphItem {
  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = at.semanticCheck
}

final case class SourceGraphItem(as: Option[Variable])(val position: InputPosition) extends SingleGraphItem {
  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = SemanticCheckResult.success
}

final case class TargetGraphItem(as: Option[Variable])(val position: InputPosition) extends SingleGraphItem {
  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = SemanticCheckResult.success
}

final case class GraphRefAliasItem(alias: GraphRefAlias)(val position: InputPosition) extends SingleGraphItem {
  def ref = alias.ref
  override def as = alias.as

  override def withNewName(newName: Variable) = copy(alias = alias.withNewName(newName))(position)
  override def semanticCheck: SemanticCheck = alias.semanticCheck
  override protected def inner: SemanticCheck = SemanticCheckResult.success
}

