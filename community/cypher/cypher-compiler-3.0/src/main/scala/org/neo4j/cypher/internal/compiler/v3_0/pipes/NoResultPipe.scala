package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{InternalPlanDescription, SingleRowPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable

case class NoResultPipe()(implicit val monitor: PipeMonitor) extends Pipe with RonjaPipe {

  override def internalCreateResults(state: QueryState) = Iterator.empty

  override def symbols = new SymbolTable()

  override def exists(pred: Pipe => Boolean) = pred(this)

  override def planDescriptionWithoutCardinality: InternalPlanDescription = new SingleRowPlanDescription(this.id, Seq.empty, variables)

  override def localEffects = Effects()

  override def dup(sources: List[Pipe]): Pipe = this

  override def sources: Seq[Pipe] = Seq.empty

  override def estimatedCardinality: Option[Double] = Some(1.0)

 override def withEstimatedCardinality(estimated: Double): Pipe with RonjaPipe = {
    assert(estimated == 1.0)
    this
  }
}
