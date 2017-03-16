package org.neo4j.cypher.internal.compiler.v3_2.bork;

public class Morsel implements Register {
    private final RegisterInfo registerInfo;
    private final long[] longs;
    private final Object[] objs;
    private final int size;
    private final boolean[] validFlag;

    private int position = -1;
    private int highestUsed = -1;

    public Morsel(RegisterInfo info, int numerOfRows) {
        this.size = numerOfRows;
        this.longs = new long[info.longSize() * numerOfRows];
        this.objs = new Object[info.longSize() * numerOfRows];
        this.registerInfo = info;
        this.validFlag = new boolean[numerOfRows];
    }


    public boolean createNext() {
        highestUsed += 1;
        if (highestUsed == size) {
            return false;
        }
        position = highestUsed;
        validFlag[highestUsed] = true;
        return true;
    }

    public boolean next() {
        while (position < (highestUsed - 1)) {
            position += 1;
            if (validFlag[position]) {
                return true;
            }
        }

        return false;
    }

    public void setInvalid() {
        validFlag[position] = false;
    }

    public void reset() {
        position = -1;
    }

    @Override
    public long getLongAt(int offset) {
        int i = position * registerInfo.longSize() + offset;
        return longs[i];
    }

    @Override
    public void setLongAt(int offset, long node) {
        int i = position * registerInfo.longSize() + offset;
        longs[i] = node;
    }

    @Override
    public Object get(int offset) {
        int i = position * registerInfo.objSize() + offset;
        return objs[i];
    }

    @Override
    public void set(int offset, Object value) {
        int i = position * registerInfo.objSize() + offset;
        objs[i] = value;
    }
}
