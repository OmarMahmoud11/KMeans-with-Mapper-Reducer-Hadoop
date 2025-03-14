package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.io.Text;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class KMeansFinalMapper extends Mapper<Object, Text, IntWritable, Text> {

    private Vector[] centroids;
    private int d, k;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration config = context.getConfiguration();
        Path centroidsFilePath = new Path("/user/omar/kmeans/centroids.txt");
        FileSystem fs = FileSystem.get(config);
        BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(centroidsFilePath))
        );
        d = config.getInt("d", 4);
        k = config.getInt("k", 3);

        centroids = new Vector[k];
        // reading first k centroids
        for (int i = 0; i < k; i++) {
            String[] parts = br.readLine().split("\t");
            if (parts.length != 2) {
                throw new IOException("Invalid centroid format at line " + (i + 1));
            }

            String vectorStr = parts[1].replaceAll("[\\[\\]]", "").trim();
            String[] values = vectorStr.split(",");


            if (values.length != d) {
                throw new IOException("Invalid dimension count at line " + (i + 1));
            }

            float[] vals = new float[d];
            for (int j = 0; j < d; j++) {
                vals[j] = Float.parseFloat(values[j]);
            }
            centroids[i] = new Vector(d, vals);
        }
        br.close();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // vector extraction
        Vector v = new Vector(d);
        StringTokenizer stringTokenizer = new StringTokenizer(value.toString(), ",");
        float[] values = new float[d];
        for (int i = 0; i < d; i++) {
            float f = Float.parseFloat(stringTokenizer.nextToken());
            values[i] = f;
        }
        v.set(values);

        // choosing nearest cluster
        float minDist = Float.MAX_VALUE;
        int nearestCluster = -1;
        for (int i = 0; i < k; i++) {
            float dist = v.squaredDistanceTo(centroids[i]);
            if (dist < minDist) {
                nearestCluster = i;
                minDist = dist;
            }
        }

//        System.out.println("Assigned vector " + v + " to cluster " + nearestCluster);
        context.write(new IntWritable(nearestCluster), value);
    }
}
