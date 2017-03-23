package org.neo4j.cypher.internal.compiler.v3_2.bork;

public interface Slot {
    class LongSlot implements Slot {
        private final int offset;

        public LongSlot(int offset) {
            this.offset = offset;
        }

        public int getOffset() {
            return offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LongSlot longSlot = (LongSlot) o;

            return offset == longSlot.offset;
        }

        @Override
        public int hashCode() {
            return offset;
        }
    }

    class RefSlot implements Slot {
        private final int offset;

        public RefSlot(int offset) {
            this.offset = offset;
        }

        public int getOffset() {
            return offset;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RefSlot refSlot = (RefSlot) o;

            return offset == refSlot.offset;
        }

        @Override
        public int hashCode() {
            return offset;
        }
    }
}