/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package cypher.feature.parser;

import cypher.feature.parser.generated.FeatureResultsBaseListener;
import cypher.feature.parser.generated.FeatureResultsParser;
import cypher.feature.parser.matchers.*;

import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

class CypherMatchersCreator extends FeatureResultsBaseListener
{
    private Deque<ValueMatcher> workload;
    private Deque<Integer> listCounters;
    private Deque<Integer> mapCounters;
    private Deque<String> keys;
    private Deque<String> names;

    private static final String INFINITY = "Inf";

    CypherMatchersCreator()
    {
        this.workload = new LinkedList<>();
        this.keys = new LinkedList<>();
        this.listCounters = new LinkedList<>();
        this.mapCounters = new LinkedList<>();
        this.names = new LinkedList<>();
    }

    ValueMatcher parsed()
    {
        return workload.pop();
    }

    @Override
    public void enterInteger( FeatureResultsParser.IntegerContext ctx )
    {
        workload.push( new IntegerMatcher( Long.valueOf( ctx.getText() ) ) );
    }

    @Override
    public void enterNullValue( FeatureResultsParser.NullValueContext ctx )
    {
        workload.push( ValueMatcher.NULL_MATCHER );
    }

    @Override
    public void enterFloatingPoint( FeatureResultsParser.FloatingPointContext ctx )
    {
        String text = ctx.getText();
        if ( text.contains( INFINITY ) )
        {
            workload.push( new FloatMatcher( Double.parseDouble( text + "inity" ) ) );
        }
        else
        {
            workload.push( new FloatMatcher( Double.parseDouble( text ) ) );
        }
    }

    @Override
    public void enterBool( FeatureResultsParser.BoolContext ctx )
    {
        workload.push( new BooleanMatcher( Boolean.valueOf( ctx.getText() ) ) );
    }

    @Override
    public void enterString( FeatureResultsParser.StringContext ctx )
    {
        String text = ctx.getText();
        String substring = text.substring( 1,
                text.length() - 1 ); // remove wrapping quotes -- because I can't get the parser rules correct :(
        String escaped = substring.replace( "\\'", "'" ); // remove escaping backslash -- see above comment
        workload.push( new StringMatcher( escaped ) );
    }

    @Override
    public void enterList( FeatureResultsParser.ListContext ctx )
    {
        listCounters.push( 0 );
    }

    @Override
    public void enterListElement( FeatureResultsParser.ListElementContext ctx )
    {
        listCounters.push( listCounters.pop() + 1 );
    }

    @Override
    public void exitList( FeatureResultsParser.ListContext ctx )
    {
        // Using a LinkedList here in order to be able to prepend
        LinkedList<ValueMatcher> temp = new LinkedList<>();
        int counter = listCounters.pop();
        for ( int i = 0; i < counter; ++i )
        {
            temp.addFirst( workload.pop() );
        }
        workload.push( new ListMatcher( temp ) );
    }

    @Override
    public void enterPropertyMap( FeatureResultsParser.PropertyMapContext ctx )
    {
        mapCounters.push( 0 );
    }

    @Override
    public void enterKeyValuePair( FeatureResultsParser.KeyValuePairContext ctx )
    {
        mapCounters.push( mapCounters.pop() + 1 );
    }

    @Override
    public void exitPropertyMap( FeatureResultsParser.PropertyMapContext ctx )
    {
        int counter = mapCounters.pop();
        Map<String,ValueMatcher> map = new HashMap<>();
        for ( int i = 0; i < counter; ++i )
        {
            ValueMatcher value = workload.pop();
            String key = keys.pop();
            map.put( key, value );
        }
        workload.push( new MapMatcher( map ) );
    }

    @Override
    public void enterPropertyKey( FeatureResultsParser.PropertyKeyContext ctx )
    {
        keys.push( ctx.getText() );
    }

    @Override
    public void enterLabelName( FeatureResultsParser.LabelNameContext ctx )
    {
        names.push( ctx.getText() );
    }

    @Override
    public void exitNode( FeatureResultsParser.NodeContext ctx )
    {
        MapMatcher properties = getMapMatcher();

        Set<String> labelNames = new HashSet<>();
        while ( !names.isEmpty() )
        {
            labelNames.add( names.pop() );
        }
        workload.push( new NodeMatcher( labelNames, properties ) );
    }

    private MapMatcher getMapMatcher()
    {
        if ( workload.isEmpty() )
        {
            return MapMatcher.EMPTY;
        }
        else
        {
            return (MapMatcher) workload.pop();
        }
    }

    @Override
    public void enterRelationshipTypeName( FeatureResultsParser.RelationshipTypeNameContext ctx )
    {
        names.push( ctx.getText() );
    }

    @Override
    public void exitRelationship( FeatureResultsParser.RelationshipContext ctx )
    {
        MapMatcher properties = getMapMatcher();
        String relTypeName = names.pop();
        workload.push( new RelationshipMatcher( relTypeName, properties ) );
    }
}
