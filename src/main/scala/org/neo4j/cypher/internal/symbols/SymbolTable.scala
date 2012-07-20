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
package org.neo4j.cypher.internal.symbols

import org.neo4j.cypher.{CypherException, CypherTypeException, SyntaxException}
import collection.Map

class SymbolTable(val identifiers: Map[String, CypherType]) {
  def hasIdentifierNamed(name: String): Boolean = identifiers.contains(name)
  def size: Int = identifiers.size
  def this() = this(Map())
  def add(key: String, typ: CypherType): SymbolTable = new SymbolTable(identifiers + (key -> typ))
  def add(value: Map[String, CypherType]): SymbolTable = new SymbolTable(identifiers ++ value)
  def filter(f: String => Boolean): SymbolTable = new SymbolTable(identifiers.filterKeys(f))
  def keys: Seq[String] = identifiers.map(_._1).toSeq
  def missingSymbolTableDependencies(x: TypeSafe) = x.symbolTableDependencies.filterNot( dep => identifiers.exists(_._1 == dep))

  def evaluateType(name: String, expectedType: CypherType): CypherType = identifiers.get(name) match {
    case Some(typ) if (expectedType.isAssignableFrom(typ)) => typ
    case Some(typ) if (typ.isAssignableFrom(expectedType)) => typ
    case Some(typ)                                         => throw new CypherTypeException("Expected `%s` to be a %s but it was %s".format(name, expectedType, typ))
    case None                                              => throw new SyntaxException("Unknown identifier `%s`.".format(name))
  }

  def checkType(name: String, expectedType: CypherType): Boolean = try {
    evaluateType(name, expectedType)
    true
  } catch {
    case _:CypherException => false
  }
}