package org.neo4j.cypher.internal.compiler.v3_2.bork.operators;

import org.neo4j.cypher.internal.compiler.v3_2.bork.*;
import org.neo4j.cypher.internal.compiler.v3_2.spi.InternalResultRow;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.core.NodeProxy;

import java.util.List;
import java.util.Map;

public class ProduceResults extends Operator {
    private final List<String> columns;
    private final Map<String, Slot> loopUp;

    public ProduceResults(Operator lhs, List<String> columns, Map<String, Slot> loopUp) {
        super(lhs);
        this.columns = columns;
        this.loopUp = loopUp;
    }

    @Override
    public boolean execute(Morsel input, QueryRun query) throws KernelException {
        ResultRow resultRow = new ResultRow(input, query);
        boolean IWantMore = true;

        while (IWantMore && input.next()) {
            try {
                IWantMore = query.getResultVisitor().visit(resultRow);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
        return false;
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    class ResultRow implements InternalResultRow {

        private final Register data;
        private final QueryRun query;

        public ResultRow(Register data, QueryRun query) {
            this.data = data;
            this.query = query;
        }

        @Override
        public Node getNode(String key) {
            throw new RuntimeException("apa");
        }

        @Override
        public Relationship getRelationship(String key) {
            throw new RuntimeException("apa");
        }

        @Override
        public Object get(String key) {
            Slot slot = loopUp.get(key);
            long nodeId = (long) data.get(slot);
            return new NodeProxy(null, nodeId);
        }

        @Override
        public String getString(String key) {
            throw new RuntimeException("apa");
        }

        @Override
        public Number getNumber(String key) {
            throw new RuntimeException("apa");
        }

        @Override
        public Boolean getBoolean(String key) {
            throw new RuntimeException("apa");
        }

        @Override
        public Path getPath(String key) {
            throw new RuntimeException("apa");
        }
    }
}
