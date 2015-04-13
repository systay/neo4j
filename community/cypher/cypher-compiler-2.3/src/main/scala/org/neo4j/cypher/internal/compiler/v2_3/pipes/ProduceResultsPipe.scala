package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames

case class ProduceResultsPipe(source: Pipe, columns: Seq[String])(val estimatedCardinality: Option[Double] = None)
                             (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) with RonjaPipe {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)
    input.map {
      original =>
        val m = MutableMaps.create(columns.size)
        columns.foreach {
          case (name) => m.put(name, original(name))
        }

        ExecutionContext(m)
    }
  }

  def planDescriptionWithoutCardinality = source.planDescription
    .andThen(this.id, "ProduceResults", identifiers, KeyNames(columns))

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))

  def localEffects = Effects()

  def symbols = source.symbols.filter(columns.contains)

  def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)(estimatedCardinality)
  }
}
