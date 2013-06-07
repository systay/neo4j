/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.experimental.ast

import org.neo4j.cypher.internal.parser.experimental._
import org.neo4j.cypher.internal.commands
import org.neo4j.cypher.internal.commands.{expressions => commandexpressions, values => commandvalues}

sealed trait Hint extends AstNode {
  def toLegacySchemaIndex : commands.StartItem with commands.Hint
}

case class UsingIndexHint(node: Identifier, label: Identifier, property: Identifier, token: InputToken) extends Hint {
  def toLegacySchemaIndex = commands.SchemaIndex(node.name, label.name, property.name, None)
}

case class UsingScanHint(node: Identifier, label: Identifier, token: InputToken) extends Hint {
  def toLegacySchemaIndex = commands.NodeByLabel(node.name, label.name)
}
