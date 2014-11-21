package org.neo4j.cypher.internal.helpers

import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v2_2.spi.{Operations, QueryContext}
import org.neo4j.graphdb.{Node, Relationship}
import org.scalatest.mock.MockitoSugar

trait CoreMocker extends MockitoSugar {
  def newMockedNode(id: Int) = {
    val node = mock[Node]
    when(node.getId).thenReturn(id)
    when(node.toString).thenReturn(s"node - $id")
    node
  }

  def newMockedRelationship(id: Int, startNode: Node, endNode: Node): Relationship = {
    val relationship = mock[Relationship]
    when(relationship.getId).thenReturn(id)
    when(relationship.getStartNode).thenReturn(startNode)
    when(relationship.getEndNode).thenReturn(endNode)
    when(relationship.getOtherNode(startNode)).thenReturn(endNode)
    when(relationship.getOtherNode(endNode)).thenReturn(startNode)
    val s = s"rel - (${startNode.getId})-[$id]->(${endNode.getId})"
    when(relationship.toString).thenReturn(s)
    relationship
  }

  def mockedGraph(rels: ((Int, Int), Int)*): QueryContext = {
    val query = mock[QueryContext]
    val nodeOps = mock[Operations[Node]]
    val relOps = mock[Operations[Relationship]]
    when(query.nodeOps).thenReturn(nodeOps)
    when(query.relationshipOps).thenReturn(relOps)

    val nodes = (rels.map(_._1._1) ++ rels.map(_._2)).distinct.map {
      n =>
        val node = newMockedNode(n)
        when(nodeOps.getById(n)).thenReturn(node)
        n -> node
    }.toMap

    rels.foreach {
      case ((f, r), t) =>
        val rel = newMockedRelationship(r, nodes(f), nodes(t))
        when(relOps.getById(r)).thenReturn(rel)
    }

    query
  }

  def mockedGraphWithNodes(nodeIds: Int*): QueryContext = {
    val query = mock[QueryContext]
    val nodeOps = mock[Operations[Node]]
    when(query.nodeOps).thenReturn(nodeOps)

    val allNodes = nodeIds map {
      n =>
        val node = newMockedNode(n)
        when(nodeOps.getById(n)).thenReturn(node)
        node
    }

    when(nodeOps.all).thenAnswer(new Answer[Iterator[Node]] {
      def answer(invocation: InvocationOnMock): Iterator[Node] = allNodes.iterator
    })
    query
  }
}
