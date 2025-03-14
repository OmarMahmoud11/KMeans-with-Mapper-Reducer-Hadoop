package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.HashSet;

public class KMeansDriver {

    final static int MAX_ITER = 50;

    public static void main (String[] args) throws IOException {
        Configuration config = new Configuration();
        if (args.length != 4) {
            System.out.println("Usage: KMeansDriver <input> <output> <d> <k>");
            System.exit(1);
        }
        int k = 0;
        try {
            k = Integer.parseInt(args[3]);
            if (k < 2) {throw new NumberFormatException();}
            config.setInt("k", k);
        } catch (NumberFormatException e) {
            System.out.println("K must be a positive integer >= 2");
            System.exit(1);
        }
        int d = Integer.parseInt(args[2]);
        config.setInt("d", d);

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path centroidsPath = new Path("/user/omar/kmeans/centroids.txt");
        // choose first k unique vectors as centroids
        // write centroids to a shared file
        FileSystem fs = null;
        try {
            fs = FileSystem.get(config);
            initializeCentroids(fs,inputPath,centroidsPath, k, d);
        } catch (IOException e) {
            System.out.println("IO ERROR");
            System.exit(3);
        }

        boolean converged = false;
        int iterations = 0;

        long startTime = System.nanoTime();
        while(!converged && iterations<MAX_ITER){
            iterations++;
            System.out.println("Iteration: " + iterations);

            long startIteration = System.nanoTime();
            Job job = null;
            try {
                job = Job.getInstance(config, "KMeans");
            } catch (IOException e) {
                System.out.println("Could not get job instance");
                System.exit(2);
            }
            job.setJarByClass(KMeansDriver.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Vector.class);

            try {
                FileInputFormat.addInputPath(job, inputPath);
            } catch (IOException e) {
                System.out.println("Could not read input");
            }
            FileOutputFormat.setOutputPath(job, outputPath);

            try {
                if (!job.waitForCompletion(true)) {
                    System.exit(1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            long endIteration = System.nanoTime();
            long iterationTime = endIteration - startIteration;
            System.out.println("Iteration Execution Time: " + (iterationTime / 1_000_000) + " milliseconds");

            // Check for convergence
            Path newCentroidsPath = new Path(outputPath + "/part-r-00000");
            converged = checkConvergence(fs, centroidsPath, newCentroidsPath, k);

            // Update centroids for the next iteration
            fs.delete(centroidsPath, true);
            fs.rename(newCentroidsPath, centroidsPath);
            fs.delete(outputPath, true);
        }
        long endTime = System.nanoTime();
        long runTime = endTime - startTime;
        System.out.println("K-Means finished in " + iterations + " iterations.");
        System.out.println("Execution Time: " + (runTime / 1_000_000) + " milliseconds");

        kmeansResult(config,inputPath, outputPath);
    }

    public static void initializeCentroids(FileSystem fs, Path inputPath, Path centroidsPath,int k,int d) throws IOException{
        BufferedReader br = new BufferedReader(
                new InputStreamReader(fs.open(inputPath))
        );
        BufferedWriter bw = new BufferedWriter(
                new OutputStreamWriter(fs.create(centroidsPath))
        );
        HashSet<String> prevLines = new HashSet<>();

        int i = 0;
        while (i < k) {
            String line = br.readLine();
            if (prevLines.contains(line)) {continue;}
            String[] values = line.split(",");

            bw.write(i +"\t[");
            for(int j=0 ; j<d ; j++){
                bw.write(values[j]);
                if(j != d-1){
                    bw.write(",");
                }
            }
            bw.write("]\n");
            prevLines.add(line);
            i++;
        }
        br.close();
        bw.close();
    }

    public static boolean checkConvergence(FileSystem fs, Path oldCentroidsPath, Path newCentroidsPath, int k) throws IOException{
        BufferedReader oldReader = new BufferedReader(new InputStreamReader(fs.open(oldCentroidsPath)));
        BufferedReader newReader = new BufferedReader(new InputStreamReader(fs.open(newCentroidsPath)));

        String oldLine, newLine;
        boolean converged = true;

        while ((oldLine = oldReader.readLine()) != null && (newLine = newReader.readLine()) != null) {
            if (!oldLine.equals(newLine)) {
                converged = false;
                break;
            }
        }

        oldReader.close();
        newReader.close();

        return converged;
    }

    public static void kmeansResult(Configuration config, Path inputPath, Path outputPath){
        Job job = null;
        try {
            job = Job.getInstance(config, "KMeans");
        } catch (IOException e) {
            System.out.println("Could not get job instance");
            System.exit(2);
        }
        job.setJarByClass(KMeansDriver.class);
        job.setMapperClass(KMeansFinalMapper.class);
//        job.setReducerClass(KMeansReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        try {
            FileInputFormat.addInputPath(job, inputPath);
        } catch (IOException e) {
            System.out.println("Could not read input");
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        try {
            if (!job.waitForCompletion(true)) {
                System.exit(1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
