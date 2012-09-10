/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands.expressions
import collection.Map
import org.neo4j.graphdb.Path
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.symbols._
import collection.JavaConverters._

case class RelationshipFunction(path: Expression) extends NullInNullOutExpression(path) {
  def compute(value: Any, m: Map[String, Any]) = value match {
    case p: Path => p.relationships().asScala.toSeq
    case x       => throw new SyntaxException("Expected " + path + " to be a path.")
  }

  def rewrite(f: (Expression) => Expression) = f(RelationshipFunction(path.rewrite(f)))

  def filter(f: (Expression) => Boolean) = if (f(this))
    Seq(this) ++ path.filter(f)
  else
    path.filter(f)

  def calculateType(symbols: SymbolTable) = {
    path.evaluateType(PathType(), symbols)
    new CollectionType(RelationshipType())
  }

  def symbolTableDependencies = path.symbolTableDependencies
}