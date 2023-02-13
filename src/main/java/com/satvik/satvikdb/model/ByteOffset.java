package com.satvik.satvikdb.model;

import java.io.Serializable;

public class ByteOffset implements Serializable {
    private long valueLengthStart;
    private long valueStart;

    public long getValueLengthStart() {
        return valueLengthStart;
    }

    public void setValueLengthStart(long valueLengthStart) {
        this.valueLengthStart = valueLengthStart;
    }

    public long getValueStart() {
        return valueStart;
    }

    public void setValueStart(long valueStart) {
        this.valueStart = valueStart;
    }

    public ByteOffset(long valueLengthStart, long valueStart) {
        this.valueLengthStart = valueLengthStart;
        this.valueStart = valueStart;
    }
}
