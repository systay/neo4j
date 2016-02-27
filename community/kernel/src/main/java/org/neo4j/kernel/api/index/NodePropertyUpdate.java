package org.neo4j.kernel.api.index;

import org.neo4j.kernel.impl.api.index.UpdateMode;

public interface NodePropertyUpdate
{
    long getNodeId();

    int getPropertyKeyId();

    Object getValueBefore();

    Object getValueAfter();

    int getNumberOfLabelsBefore();

    int getLabelBefore( int i );

    int getNumberOfLabelsAfter();

    int getLabelAfter( int i );

    UpdateMode getUpdateMode();

    boolean forLabel( long labelId );

    class NodePropertyUpdateDelegator implements NodePropertyUpdate {
        private NodePropertyUpdate delegate;

        public NodePropertyUpdateDelegator( NodePropertyUpdate delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public long getNodeId()
        {
            return delegate.getNodeId();
        }

        @Override
        public int getPropertyKeyId()
        {
            return delegate.getPropertyKeyId();
        }

        @Override
        public Object getValueBefore()
        {
            return delegate.getValueBefore();
        }

        @Override
        public Object getValueAfter()
        {
            return delegate.getValueAfter();
        }

        @Override
        public int getNumberOfLabelsBefore()
        {
            return delegate.getNumberOfLabelsBefore();
        }

        @Override
        public int getLabelBefore( int i )
        {
            return delegate.getLabelBefore( i );
        }

        @Override
        public int getNumberOfLabelsAfter()
        {
            return delegate.getNumberOfLabelsAfter();
        }

        @Override
        public int getLabelAfter( int i )
        {
            return delegate.getLabelAfter( i );
        }

        @Override
        public UpdateMode getUpdateMode()
        {
            return delegate.getUpdateMode();
        }

        @Override
        public boolean forLabel( long labelId )
        {
            return delegate.forLabel( labelId );
        }
    }
}
