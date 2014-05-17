/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pprint.docgen

import org.neo4j.cypher.internal.compiler.v2_1.pprint._
import scala.collection.immutable
import scala.collection.mutable
import org.neo4j.cypher.internal.compiler.v2_1.pprint.impl.{quoteString, quoteChar}

case object scalaDocGenerator extends NestedDocGenerator[Any] {

  import Doc._

  val forNestedProducts: RecursiveDocGenerator[Any] = {
    case p: Product if p.productArity == 0 => (inner) =>
      productPrefix(p)

    case p: Product => (inner) =>
      block(productPrefix(p))(sepList(p.productIterator.map(inner)))
  }

  val forNestedMaps: RecursiveDocGenerator[Any] = {
    case m: mutable.Map[_, _] => (inner) =>
      val mapType = m.getClass.getSimpleName
      val innerDocs = m.map { case (k, v) => nest(group(inner(k) :/: "→ " :: inner(v))) }
      block(mapType)(sepList(innerDocs))

    case m: immutable.Map[_, _] => (inner) =>
      val innerDocs = m.map { case (k, v) => nest(group(inner(k) :/: "→ " :: inner(v))) }
      block("Map")(sepList(innerDocs))
  }

  val forNestedSets: RecursiveDocGenerator[Any] = {
    case s: mutable.Set[_] => (inner) =>
      val setType = s.getClass.getSimpleName
      val innerDocs = s.map(inner)
      block(setType)(sepList(innerDocs))

    case s: immutable.Set[_] => (inner) =>
      val innerDocs = s.map(inner)
      block("Set")(sepList(innerDocs))
  }

  val forNestedSequences: RecursiveDocGenerator[Any] = {
    case s: Seq[_] => (inner) =>
      val seqType = s.getClass.getSimpleName
      val innerDocs = s.map(inner)
      block(seqType)(sepList(innerDocs))
  }

  val forNestedArrays: RecursiveDocGenerator[Any] = {
    case a: Array[_] => (inner) =>
      val innerDocs = a.map(inner)
      block("Array")(sepList(innerDocs))
  }

  val forNestedLists: RecursiveDocGenerator[Any] = {
    case l: List[_] => (inner) =>
      group( l.foldRight[Doc]("⬨") { case (v, doc) => inner(v) :/: "⸬ " :: doc } )
  }

  val forNestedPrimitiveValues: RecursiveDocGenerator[Any] = {
    case v: String => (inner) =>
      quoteString(v)

    case ch: Char => (inner) =>
      quoteChar(ch)
  }

  protected val instance: RecursiveDocGenerator[Any] =
    forNestedPrimitiveValues orElse
    forNestedArrays orElse
    forNestedMaps orElse
    forNestedLists orElse
    forNestedSets orElse
    forNestedSequences orElse
    forNestedProducts

  private def productPrefix(p: Product) = {
    val prefix = p.productPrefix
    if (prefix.startsWith("Tuple")) "" else prefix
  }
}

