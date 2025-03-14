package org.example;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class Vector implements Writable {
    private int DIM;
    private float[] values;

    // necessary for Hadoop
    public Vector() {}

    public Vector(int dim) {
        this.DIM = dim;
        values = new float[DIM];
    }

    public Vector(int dim,float[] values) {
        this.DIM = dim;
//        values = new float[DIM];
//        set(values);
        this.values = values;
    }

    public int getDim() {
        return DIM;
    }

    public void set(float[] values) {
        for (int i = 0; i < DIM; i++) {
            this.values[i] = values[i];
        }
    }

    public float[] get() {
        return values;
    }

    public float squaredDistanceTo(Vector v) {
        float sum = 0;
        for (int i = 0; i < DIM; i++) {
            float diff = values[i] - v.values[i];
            sum += diff * diff;
        }
        return sum;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(DIM);
        for (int i = 0; i < DIM; i++) {
            dataOutput.writeFloat(values[i]);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        DIM = dataInput.readInt();
        values = new float[DIM];
        for (int i = 0; i < DIM; i++) {
            values[i] = dataInput.readFloat();
        }
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }
}
