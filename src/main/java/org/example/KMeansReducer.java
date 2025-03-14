package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KMeansReducer extends Reducer<IntWritable, Vector, IntWritable, Vector> {
    private int d, k;

    @Override
    protected void setup(Reducer<IntWritable, Vector, IntWritable, Vector>.Context context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        d = config.getInt("d", 4);
        k = config.getInt("k", 3);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Vector> values, Context context) throws IOException, InterruptedException {
        Vector newCentroid = new Vector(d, new float[d]);
        int clusterSize = 0;
        for (Vector v: values) {
            for (int i = 0; i < d; i++) {
                newCentroid.get()[i] += v.get()[i];
            }
            clusterSize++;
        }

        for (int i = 0; i < d; i++) {
            newCentroid.get()[i] /= clusterSize;
        }

        context.write(key, newCentroid);
    }
}
