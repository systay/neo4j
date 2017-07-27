/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.PrimitiveExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ExecutionContext, PipelineInformation}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.InternalException

import scala.collection.mutable


case class EagerRegisterPipe(source: Pipe, pipelineInformation: PipelineInformation)(val id: Id = new Id)
  extends PipeWithSource(source) {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val buffer = mutable.ListBuffer.newBuilder[Bucket]
    val longSize = pipelineInformation.numberOfLongs


    { // Materialize input into buckets
      var currentSize = 32
      var currentBucket = new Bucket(currentSize)
      var currentIndex = 0
      while (input.nonEmpty) {
        if (currentIndex == currentSize) {
          currentBucket.highestUsed = currentIndex - 1
          currentIndex = 0
          currentSize = (currentSize * 2) % 65535
          buffer += currentBucket
          currentBucket = new Bucket(currentSize)
        }

        val currentRow = input.next().asInstanceOf[PrimitiveExecutionContext]
        System.arraycopy(currentRow.longs, 0, currentBucket.longs, currentIndex * longSize, longSize)
        currentIndex += 1
      }
      currentBucket.highestUsed = currentIndex - 1
      buffer += currentBucket
    }

    // Data has been loaded. Let's return an iterator over it

    new Iterator[ExecutionContext] {

      val buckets = buffer.result().iterator
      var empty = false
      var currentIndexInBucket = 0
      var currentBucket: Bucket = _

      loadNext()

      override def hasNext: Boolean = !empty

      private def loadNext() {
        if(buckets.isEmpty)
          empty = true
        else {
          currentBucket = buckets.next()
          if (currentBucket.highestUsed == -1)
            empty = true
        }
      }

      override def next(): ExecutionContext = {
        if(empty)
          Iterator.empty.next() // will throw the appropriate exception

        val result = currentBucket.executionContextToIndex(currentIndexInBucket)


        if(currentIndexInBucket == currentBucket.highestUsed) {
          if(buckets.isEmpty)
            empty = true
          else {
            currentBucket = buckets.next()
            currentIndexInBucket = 0
            if (currentBucket.highestUsed == -1)
              empty = true
          }
        } else {
          currentIndexInBucket += 1
        }

        result
      }
    }
  }


  class Bucket(rows: Int) {
    var highestUsed = 0
    val longs = new Array[Long](rows * pipelineInformation.numberOfLongs)
    //      val refs = new Array[AnyRef](rows * pipelineInformation.numberOfReferences)
    def executionContextToIndex(index: Int): ExecutionContext = new ExecutionContext {
      override def setLongAt(offset: Int, value: Long): Unit = failWrite()

      override def copyFrom(input: ExecutionContext): Unit = failWrite()

      override def getLongAt(offset: Int): Long = {
        val actualOffset = index * pipelineInformation.numberOfLongs + offset
        longs(actualOffset)
      }

      override def newWith(newEntries: Seq[(String, Any)]): ExecutionContext = failMap()

      override def createClone(): ExecutionContext = failMap()

      override def newWith1(key1: String, value1: Any): ExecutionContext = failMap()

      override def newWith2(key1: String, value1: Any, key2: String, value2: Any): ExecutionContext = failMap()

      override def newWith3(key1: String, value1: Any, key2: String, value2: Any, key3: String, value3: Any): ExecutionContext = failMap()

      override def mergeWith(other: ExecutionContext): ExecutionContext = failMap()

      override def +=(kv: (String, Any)): this.type = failMap()

      override def -=(key: String): this.type = failMap()

      override def iterator: Iterator[(String, Any)] = failMap()

      override def get(key: String): Option[Any] = failMap()

      private def failMap(): Nothing = throw new InternalException("Tried using a primitive context as a map")
      private def failWrite(): Nothing = throw new InternalException("Read only row cannot be written to")
    }
  }

}

