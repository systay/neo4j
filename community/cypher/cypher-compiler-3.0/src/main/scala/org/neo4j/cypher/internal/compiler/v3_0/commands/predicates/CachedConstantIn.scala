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
package org.neo4j.cypher.internal.compiler.v3_0.commands.predicates

import java.util.Map.Entry

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.helpers.{CollectionSupport, RuntimeJavaValueConverter}
import org.neo4j.cypher.internal.compiler.v3_0.pipes.QueryState
import org.neo4j.graphdb.{Node, Path, Relationship}

import scala.collection.mutable

/*
This class is used for making the common <exp> IN <constant-expression> fast

It uses a cache for the <constant-expression> value, and turns it into a Set, for fast existence checking
 */
abstract class CachedIn(value: Expression, list: Expression) extends Predicate with CollectionSupport {

  // These two are here to make the fields accessible without conflicting with the case classes
  protected def valueExp = value
  protected def listExp = list

  protected def getSet(ctx: ExecutionContext)(implicit state: QueryState): Set[Any]

  override def isMatch(ctx: ExecutionContext)(implicit state: QueryState) = {
    val converter = new RuntimeJavaValueConverter(state.query.isGraphKernelResultValue, state.publicTypeConverter)
    val setToCheck: Set[Any] = getSet(ctx)

    val value1 = converter.asDeepJavaValue(value(ctx))
    val result = Option(value1) map setToCheck.apply


    // WHERE n.prop IN [1,2,3]

    // When checking if a value is contained in a collection that contains null, the result is null, and not false if
    // the value is not present in the collection
    if (result.contains(false) && setToCheck.contains(null))
      None
    else
      result
  }

  override def containsIsNull = false

  override def arguments = Seq(list)

  override def symbolTableDependencies = list.symbolTableDependencies ++ value.symbolTableDependencies
}


class Check(var checker: ContainsFunction, list: Iterator[Any]) {
  def contains(value: Any): Option[Boolean] = {
    val (result, newChecker) = checker.contains(value)
    checker = newChecker
    result
  }
}





object CachedIn {
  def unapply(exp: Expression): Option[(Expression, Expression)] = exp match {
    case exp: CachedIn => Some((exp.valueExp, exp.listExp))
    case _ => None
  }
}

/*
This class is used for making the common <exp> IN <constant-expression> fast

It uses a cache for the <constant-expression> value, and turns it into a Set, for fast existence checking
 */
case class CachedConstantIn(value: Expression, list: Expression) extends CachedIn(value, list) {
  override protected def getSet(ctx: ExecutionContext)(implicit state: QueryState): Set[Any] = {
    val converterA = new RuntimeJavaValueConverter(state.query.isGraphKernelResultValue, state.publicTypeConverter)
    val traversable: Iterable[Any] = makeTraversable(list(ctx))
    val apa = converterA.asDeepJavaValue(traversable)
    val apa2 = converterA.asDeepJavaValue(list(ctx))
    state.cachedIn.getOrElseUpdate(list, Set.empty)
  }

  override def rewrite(f: (Expression) => Expression) = f(CachedConstantIn(value.rewrite(f), list.rewrite(f)))
}

/*
This class is used for making the common <exp> IN <expression> fast

It uses an LRU cache for the <expression> value, and turns it into a Set, for fast existence checking
 */
case class CachedDynamicIn(value: Expression, list: Expression) extends CachedIn(value, list) {
  override protected def getSet(ctx: ExecutionContext)(implicit state: QueryState): Set[Any] = {
    val listValue = makeTraversable(list(ctx))

    val setToCheck: Set[Any] = state.cachedIn.getOrElseUpdate(listValue, {
      val set = listValue.toSet
      set
    })
    setToCheck
  }

  override def rewrite(f: (Expression) => Expression) = f(CachedConstantIn(value.rewrite(f), list.rewrite(f)))
}


class SingleThreadedLRUCache[K,V](maxSize: Int) extends java.util.LinkedHashMap[K,V](maxSize, 0.75f, true) {
  override def removeEldestEntry(eldest: Entry[K, V]): Boolean = size() < maxSize

  def getOrElseUpdate(key: K, f: => V): V = {
    val kToV = new java.util.function.Function[K,V] {
      override def apply(v1: K): V = f
    }

    computeIfAbsent(key, kToV)
  }
}

