package org.neo4j.cypher.internal.compiler.v3_0.pipes

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.{InternalPlanDescription, SingleRowPlanDescription}
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable

case class SingleRowPipe()(implicit val monitor: PipeMonitor) extends Pipe with RonjaPipe {

  def symbols: SymbolTable = new SymbolTable()

  def internalCreateResults(state: QueryState) =
    Iterator(state.initialContext.getOrElse(ExecutionContext.empty))

  def exists(pred: Pipe => Boolean) = pred(this)

  def planDescriptionWithoutCardinality: InternalPlanDescription = new SingleRowPlanDescription(this.id, Seq.empty, variables)

  override def localEffects = Effects()

  def dup(sources: List[Pipe]): Pipe = this

  def sources: Seq[Pipe] = Seq.empty

  def estimatedCardinality: Option[Double] = Some(1.0)

  def withEstimatedCardinality(estimated: Double): Pipe with RonjaPipe = {
    assert(estimated == 1.0)
    this
  }
}
