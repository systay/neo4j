/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3

import java.io.PrintWriter
import java.lang.{Boolean, Double, Long}
import java.util
import java.util.Collections

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.helpers.{CollectionSupport, Eagerly}
import org.neo4j.cypher.internal.compiler.v2_3.notification.InternalNotification
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.{ExecutionMode, ExplainMode, ProfileMode}
import org.neo4j.graphdb.QueryExecutionType.{QueryType, profiled, query}
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.graphdb.{Node, Path, Relationship, ResourceIterator}

import scala.collection.JavaConverters._
import scala.collection.Map

//TODO: Do this for reals
case class MapRowAdapter(m: Map[String,Any]) extends ResultRow {
  def getNode(key: String): Node = m(key).asInstanceOf[Node]

  def getRelationship(key: String): Relationship = m(key).asInstanceOf[Relationship]

  def get(key: String): AnyRef = m(key).asInstanceOf[AnyRef]

  def getDouble(key: String): Double = m(key).asInstanceOf[Double]

  def getLong(key: String): Long = m(key).asInstanceOf[Long]

  def getBoolean(key: String): Boolean = m(key).asInstanceOf[Boolean]

  def getPath(key: String): Path = m(key).asInstanceOf[Path]

  def getString(key: String): String = m(key).asInstanceOf[String]
}

class PipeExecutionResult(val result: ResultIterator,
                          val columns: List[String],
                          val state: QueryState,
                          val executionPlanBuilder: () => InternalPlanDescription,
                          val executionMode: ExecutionMode,
                          val queryType: QueryType)
  extends InternalExecutionResult
  with CollectionSupport {

  self =>

  def accept(visitor: ResultVisitor) = {
    var cont = true
    while (cont && result.hasNext) {
      val map = result.next()
      cont = visitor.visit(MapRowAdapter(map))
    }
  }

  lazy val dumpToString = withDumper(dumper => dumper.dumpToString(_))

  def dumpToString(writer: PrintWriter) { withDumper(dumper => dumper.dumpToString(writer)(_)) }

  def executionPlanDescription(): InternalPlanDescription = executionPlanBuilder()

  def javaColumns: java.util.List[String] = columns.asJava

  def javaColumnAs[T](column: String): ResourceIterator[T] = new WrappingResourceIterator[T] {
    def hasNext = self.hasNext
    def next() = makeValueJavaCompatible(getAnyColumn(column, self.next())).asInstanceOf[T]
  }

  def columnAs[T](column: String): Iterator[T] = map { case m => getAnyColumn(column, m).asInstanceOf[T] }

  def javaIterator: ResourceIterator[java.util.Map[String, Any]] = new WrappingResourceIterator[util.Map[String, Any]] {
    def hasNext = self.hasNext
    def next() = Eagerly.immutableMapValues(self.next(), makeValueJavaCompatible).asJava
  }

  override def toList: List[Predef.Map[String, Any]] = result.toList

  def hasNext = result.hasNext

  def next() = result.next()

  def queryStatistics() = state.getStatistics

  def close() { result.close() }

  def planDescriptionRequested = executionMode == ExplainMode || executionMode == ProfileMode

  private def getAnyColumn[T](column: String, m: Map[String, Any]): Any = {
    m.getOrElse(column, {
      throw new EntityNotFoundException("No column named '" + column + "' was found. Found: " + m.keys.mkString("(\"", "\", \"", "\")"))
    })
  }

  private def makeValueJavaCompatible(value: Any): Any = value match {
    case iter: Seq[_]    => iter.map(makeValueJavaCompatible).asJava
    case iter: Map[_, _] => Eagerly.immutableMapValues(iter, makeValueJavaCompatible).asJava
    case x               => x
  }

  private def withDumper[T](f: (ExecutionResultDumper) => (QueryContext => T)): T = {
    val result = toList
    state.query.withAnyOpenQueryContext( qtx => f(ExecutionResultDumper(result, columns, queryStatistics()))(qtx) )
  }

  private trait WrappingResourceIterator[T] extends ResourceIterator[T] {
    def remove() { Collections.emptyIterator[T]().remove() }
    def close() { self.close() }
  }

  def executionType = if (executionMode == ProfileMode) profiled(queryType) else query(queryType)

  //notifications only present for EXPLAIN
  override val notifications = Iterable.empty[InternalNotification]
}

