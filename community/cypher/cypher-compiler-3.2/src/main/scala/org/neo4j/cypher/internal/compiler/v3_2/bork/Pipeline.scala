package org.neo4j.cypher.internal.compiler.v3_2.bork

import org.neo4j.cypher.internal.compiler.v3_2.bork.ir.IntermediateRepresentation

case class Pipeline(numberOfLongs: Int,
                    numberOfRegs: Int,
                    operators: Seq[IntermediateRepresentation],
                    dependencies: Seq[Dependency])


trait Dependency {
  def pipeline: Pipeline
}

// This is used to model the dependency an Apply has - it needs at least one row from the LHS
case class AtLeastOne(pipeline: Pipeline) extends Dependency
case class All(pipeline: Pipeline) extends Dependency