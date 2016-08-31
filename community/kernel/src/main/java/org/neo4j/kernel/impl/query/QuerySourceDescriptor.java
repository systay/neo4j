package org.neo4j.kernel.impl.query;

public class QuerySourceDescriptor
{
    private final String[] parts;

    public QuerySourceDescriptor( String ... parts )
    {
        this.parts = parts;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder(  );

        boolean isFirst = true;
        for ( String part : parts )
        {
            if ( isFirst )
            {
                isFirst = false;
            }
            else
            {
                builder.append( '\t' );
            }
            builder.append( part );
        }

        return builder.toString();
    }
}
