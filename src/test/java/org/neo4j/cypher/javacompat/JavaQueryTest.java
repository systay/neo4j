/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.javacompat;

import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.neo4j.test.JavaDocsGenerator;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public class JavaQueryTest
{
    @Test
    public void test()
    {
        JavaDocsGenerator gen = new JavaDocsGenerator( "java-cypher-queries", "dev/java" );
        
        JavaQuery jq = new JavaQuery();
        jq.run();
        assertTrue(jq.columnsString.contains( "n" ));
        assertTrue(jq.resultString.contains( "Node[0]{}" ));
        assertTrue(jq.resultString.contains( "1 rows" ));
        assertTrue(jq.resultNode.getId() == 0L);

        gen.saveToFile( "target/docs", "result",
                AsciidocHelper.createOutputSnippet( jq.resultString ) );
        gen.saveToFile( "target/docs", "columns",
                AsciidocHelper.createOutputSnippet( jq.columnsString ) );
        gen.saveToFile( "target/docs", "node",
                AsciidocHelper.createOutputSnippet( jq.resultNode.toString() ) );
    }
}
