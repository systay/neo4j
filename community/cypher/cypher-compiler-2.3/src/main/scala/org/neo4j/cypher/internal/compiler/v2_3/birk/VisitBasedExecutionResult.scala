package org.neo4j.cypher.internal.compiler.v2_3.birk

import java.io.PrintWriter
import java.util

import org.neo4j.cypher.internal.compiler.v2_3.InternalQueryStatistics
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.notification.InternalNotification
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.graphdb.{QueryExecutionType, ResourceIterator}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


abstract class VisitBasedExecutionResult extends InternalExecutionResult {

  private val eagerResult = {
    new mutable.ListBuffer[]
  }

  override def columns: List[String] = ???

  override def javaIterator: ResourceIterator[util.Map[String, Any]] = ???

  override def executionType: QueryExecutionType = ???

  override def columnAs[T](column: String): Iterator[T] = ???

  override def queryStatistics(): InternalQueryStatistics = ???

  override def dumpToString(writer: PrintWriter): Unit = ???

  override def dumpToString(): String = ???

  override def javaColumnAs[T](column: String): ResourceIterator[T] = ???

  override def executionPlanDescription(): InternalPlanDescription = ???

  override def close(): Unit = ???

  override def notifications: Iterable[InternalNotification] = ???

  override def planDescriptionRequested: Boolean = ???

  override def next(): Map[String, Any] = ???

  override def hasNext: Boolean = ???
}
