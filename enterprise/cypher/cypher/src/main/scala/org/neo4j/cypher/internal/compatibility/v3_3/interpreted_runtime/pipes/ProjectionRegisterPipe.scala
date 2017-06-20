package org.neo4j.cypher.internal.compatibility.v3_3.interpreted_runtime.pipes

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeMonitor, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id

case class ProjectionRegisterPipe(source: Pipe, columns: Seq[(Int, Expression)])
                                 (val id: Id = new Id)
                                 (implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RegisterPipe {

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input map {
      ctx =>
        columns foreach {
          case (offset, expression) =>
            val value = expression(ctx)(state)
            ctx.setRef(offset, value.asInstanceOf[AnyRef])
        }
        ctx
    }
  }

}