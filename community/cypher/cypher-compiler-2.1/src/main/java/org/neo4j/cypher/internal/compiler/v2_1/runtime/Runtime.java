/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_1.runtime;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.cypher.internal.compiler.v2_1.runtime.Register;
import java.util.ArrayList;

public class Runtime {
    private GraphDatabaseService graph;
    private ArrayList<Register<Long>> idRegisters;
    private ArrayList<Register<Object>> objectRegisters;

    public Runtime(GraphDatabaseService graph)
    {
        this.graph = graph;
        idRegisters = new ArrayList<Register<Long>>();
        objectRegisters = new ArrayList<Register<Object>>();
    }

    public void allocateIdRegisters(int count)
    {
        idRegisters.clear();
        for (int i=0; i < count; i++)
            idRegisters.add(new Register<Long>());
    }

    public int getIdRegistersCount()
    {
        return idRegisters.size();
    }

    public int getObjectRegisterCount()
    {
        return objectRegisters.size();
    }

    public void allocateObjectRegisters(int count)
    {
        objectRegisters.clear();
        for (int i=0; i < count; i++)
            objectRegisters.add(new Register<Object>());
    }

    public Register getIdRegister(int slot){
        return idRegisters.get(slot);
    }

    public Register getObjectRegister(int slot){
        return objectRegisters.get(slot);
    }

}
