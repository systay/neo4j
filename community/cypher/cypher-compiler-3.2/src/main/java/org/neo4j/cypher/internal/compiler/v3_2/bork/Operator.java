package org.neo4j.cypher.internal.compiler.v3_2.bork;

import java.util.Optional;

public abstract class Operator {
    public abstract boolean execute(Morsel input, QueryRun query);

    public Operator(Operator lhs, Operator rhs) {
        this.lhs = Optional.of(lhs);
        this.rhs = Optional.of(rhs);
    }

    public Operator(Operator lhs) {
        this.lhs = Optional.of(lhs);
        this.rhs = Optional.empty();
    }

    public Operator() {
        this.lhs = Optional.empty();
        this.rhs = Optional.empty();
    }

    public Optional<Operator> lhs;
    public Optional<Operator> rhs;
    public Optional<Operator> parent = Optional.empty();

    public void becomeParent() {
        lhs.ifPresent(op -> op.setParent(this));
        rhs.ifPresent(op -> op.setParent(this));
    }

    protected void setParent(Operator parent) {
        this.parent = Optional.of(parent);
        becomeParent();
    }
}


