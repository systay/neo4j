package org.neo4j.cypher.internal.compiler.v3_2.bork

import org.neo4j.cypher.internal.compiler.v3_2.bork.Slot.{LongSlot, RefSlot}
import org.neo4j.cypher.internal.compiler.v3_2.bork.ir.IntermediateRepresentation
import org.neo4j.cypher.internal.ir.v3_2.IdName

import scala.collection.mutable

/**
  * Contains information about how to map variables to slots, and what registers need to be created for the pipeline
  */
class PipeLineBuilder(var slots: Map[IdName, Slot] = Map.empty,
                      var numberOfLongs: Int = 0,
                      var numberOfReferences: Int = 0,
                      operatorBuilder: mutable.ListBuffer[IntermediateRepresentation] = new mutable.ListBuffer[IntermediateRepresentation](),
                      dependencyBuilder: mutable.ListBuffer[Dependency] = new mutable.ListBuffer[Dependency]()) {
  def addLong(id: IdName): Int = {
    val newSlot = new LongSlot(numberOfLongs)
    slots = slots + (id -> newSlot)
    numberOfLongs = numberOfLongs + 1
    newSlot.getOffset
  }

  def addObj(id: IdName): Int = {
    val newSlot = new RefSlot(numberOfReferences)
    slots = slots + (id -> newSlot)
    numberOfReferences = numberOfReferences + 1
    newSlot.getOffset
  }

  def addOperator(operator: IntermediateRepresentation): PipeLineBuilder = {
    operatorBuilder += operator
    this
  }

  def build(): Pipeline = {
    new Pipeline(numberOfLongs, numberOfReferences, operatorBuilder, Seq.empty)
  }
}

