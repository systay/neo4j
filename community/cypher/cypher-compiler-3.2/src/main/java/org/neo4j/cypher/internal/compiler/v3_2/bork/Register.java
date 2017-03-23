package org.neo4j.cypher.internal.compiler.v3_2.bork;

public interface Register
{
    long getLongAt(int idx);
    void setLongAt(int idx, long value);
    Object get(int idx);
    void set(int idx, Object value);
    Object get(Slot slot);
}
