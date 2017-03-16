package org.neo4j.cypher.internal.compiler.v3_2.bork;

public class RegisterInfo
{
    private int _numberOfLongs;
    private int _numberOfObjects;

    public int longSize()
    {
        return _numberOfLongs;
    }

    public int objSize()
    {
        return _numberOfObjects;
    }

    public RegisterInfo( int longSize, int objSize)
    {
        this._numberOfLongs = longSize;
        this._numberOfObjects = objSize;
    }
}
