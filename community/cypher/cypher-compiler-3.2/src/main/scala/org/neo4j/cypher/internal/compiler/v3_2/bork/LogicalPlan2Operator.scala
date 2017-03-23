package org.neo4j.cypher.internal.compiler.v3_2.bork

//class LogicalPlan2Operator(pipeLines: Map[LogicalPlan, PipeLine], semanticTable: SemanticTable)
//  extends TreeBuilder[PipeLineBuilder] {
//
//  implicit val s = semanticTable
//
//  override def create(plan: LogicalPlan): (Operator, Map[Operator, PipeLine]) = {
//    val (operator, pipelines) = super.create(plan)
//    operator.becomeParent()
//    (operator, pipelines)
//  }
//
//  override protected def build(plan: LogicalPlan): (Operator, Map[Operator, PipeLine]) = {
//    implicit val pipeLine = pipeLines(plan)
//    val operator = plan match {
//      case plans.AllNodesScan(id, _) =>
//        val slot = pipeLine.slots(id)
//        assert(slot.isInstanceOf[LongSlot])
//        new AllNodesScanOp(slot.asInstanceOf[LongSlot].getOffset)
//
//      case plans.NodeByLabelScan(id, labelName, _) =>
//        val slot = pipeLine.slots(id)
//        assert(slot.isInstanceOf[LongSlot])
//        val tokenId = labelName.id.get.id
//        new NodeByLabelScanOp(slot.asInstanceOf[LongSlot].getOffset, tokenId)
//    }
//
//    (operator, Map(operator -> pipeLine))
//  }
//
//  override protected def build(plan: LogicalPlan, in: (Operator, Map[Operator, PipeLine])): (Operator, Map[Operator, PipeLine]) = {
//    implicit val pipeLine = pipeLines(plan)
//    val (source, mapAcc) = in
//    val operator = plan match {
//      case plans.Selection(astPredicates, _) =>
//        val predicates = astPredicates.map(p => ExpressionConverter.transform(p))
//        new FilterOp(source, predicates.head) // TODO: .head here is cheating
//
//      case plans.ProduceResult(columns, _) =>
//        val java: util.List[String] = columns.toList.asJava
//
//        val toMap = pipeLine.slots.map { case (IdName(k), v) => k -> v }
//
//        new ProduceResults(source, java, mapAsJavaMap(toMap))
//    }
//    (operator, mapAcc + (operator -> pipeLine))
//  }
//
//
//  override protected def build(plan: LogicalPlan, lhs: (Operator, Map[Operator, PipeLine]), rhs: (Operator, Map[Operator, PipeLine])): (Operator, Map[Operator, PipeLine]) = ???
//}
