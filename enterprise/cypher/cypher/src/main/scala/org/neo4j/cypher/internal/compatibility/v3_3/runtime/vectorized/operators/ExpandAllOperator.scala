package org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized.operators

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.PipelineInformation
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.LazyTypes
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.vectorized._
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticDirection}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.kernel.impl.api.RelationshipVisitor
import org.neo4j.kernel.impl.api.store.RelationshipIterator

class ExpandAllOperator(toPipeline: PipelineInformation,
                        fromPipeline: PipelineInformation,
                        fromOffset: Int,
                        relOffset: Int,
                        toOffset: Int,
                        dir: SemanticDirection,
                        types: LazyTypes) extends LeafOperator {

  override def operate(source: Continuation,
                       output: Morsel,
                       context: QueryContext,
                       state: QueryState): (ReturnType, Continuation) = {

    /*
    This might look wrong, but it's like this by design. This allows the loop to terminate early and still be
    picked up at any point again - all without impacting the tight loop.
    The mutable state is an unfortunate cost for this feature.
     */
    var readPos = 0
    var writePos = 0
    var relationships: RelationshipIterator = null
    var input:  Morsel = null

    source match {
      case InitWithData(data) =>
        input = data
      case ContinueWithData(data, index) =>
        input = data
        readPos = index
      case ContinueWithDataAndSource(data, index, rels) =>
        input = data
        readPos = index
        relationships = rels.asInstanceOf[RelationshipIterator]
    }

    while (readPos < input.validRows && writePos < output.validRows) {

      val inputLongRow = readPos * fromPipeline.numberOfLongs
      val fromNode = input.longs(inputLongRow + fromOffset)

      if (fromNode == -1) // If the input node is null, no need to expand
        readPos += 1
      else {
        relationships = if (relationships == null) {
          context.getRelationshipsForIdsPrimitive(fromNode, dir, types.types(context))
        }
        else
          relationships

        var otherSide: Long = 0
        val relVisitor = new RelationshipVisitor[InternalException] {
          override def visit(relationshipId: Long, typeId: Int, startNodeId: Long, endNodeId: Long): Unit =
            if (fromNode == startNodeId)
              otherSide = endNodeId
            else
              otherSide = startNodeId
        }

        while (writePos < output.validRows && relationships.hasNext) {
          val relId = relationships.next()
          relationships.relationshipVisit(relId, relVisitor)

          // Now we have everything needed to create a row.
          val outputRow = toPipeline.numberOfLongs * writePos
          System.arraycopy(input.longs, inputLongRow, output.longs, outputRow, fromPipeline.numberOfLongs)
          System.arraycopy(input.refs, fromPipeline.numberOfReferences * readPos, output.refs, toPipeline.numberOfReferences * writePos, fromPipeline.numberOfReferences)
          output.longs(outputRow + relOffset) = relId
          output.longs(outputRow + toOffset) = otherSide
          writePos += 1
        }

        if (!relationships.hasNext) {
          relationships = null
          readPos += 1
        }
      }
    }

    val next = if (readPos < input.validRows || relationships != null) {
      if(relationships == null)
        ContinueWithData(input, readPos)
      else
        ContinueWithDataAndSource(input, readPos, relationships)
    } else
      Done

    output.validRows = writePos
    (MorselType, next)
  }

  override def init(state: QueryState, context: QueryContext): Unit = {}
}
