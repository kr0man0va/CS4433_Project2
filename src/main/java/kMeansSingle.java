import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

//Single-iteration k-means clustering algorithm
public class kMeansSingle {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        private List<String> centroids = new ArrayList<>();

        //Add setup to read centroid file in memory
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            Path path = files[0];
            // open the stream
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            // wrap it into a BufferedReader object which is easy to read a record
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis,
                    "UTF-8"));
            // read the record line by line
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] split = line.split(",");
                centroids.add(split[0] + "," + split[1]);
            }
            // close the stream
            IOUtils.closeStream(reader);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");

            //Get point coordinates
            int pointX = Integer.valueOf(values[0]);
            int pointY = Integer.valueOf(values[1]);

            //Closest centroid
            double closestDistance = Double.POSITIVE_INFINITY;
            int[] closestCentroid = new int[]{0,0};

            //Go through each centroid and calculate distance
            for(int i = 0; i < centroids.size(); i++) {

                String[] centroid = centroids.get(i).split(",");

                int centroidX = Integer.valueOf(centroid[0]);
                int centroidY = Integer.valueOf(centroid[1]);

                double distance = Math.sqrt(Math.pow(pointX-centroidX,2) + Math.pow(pointY-centroidY,2));

                if(distance < closestDistance) {
                    closestDistance = distance;
                    closestCentroid[0] = centroidX;
                    closestCentroid[1] = centroidY;
                }

            }

            Text outputK = new Text(closestCentroid[0] + "," + closestCentroid[1]);
            Text outputV = new Text(pointX + "," + pointY);

            //After we found centroid, output centroid coord as key and data point coord as value
            context.write(outputK, outputV);

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //Calculate new centroids
            int sumX = 0;
            int sumY = 0;
            int count = 0;
            for (Text x : values) {
                String[] point = x.toString().split(",");
                sumX += Integer.valueOf(point[0]);
                sumY += Integer.valueOf(point[1]);
                count += 1;
            }

            int centroidX = sumX / count;
            int centroidY = sumY / count;

            context.write(new Text(centroidX + "," + centroidY), NullWritable.get());

        }

    }

    //Pass K number/ input and output is fixed
    public static void main(String[] args) throws Exception{

        long startTime = System.currentTimeMillis();

        //Get number of centroids
//        int k = Integer.valueOf(args[0]);
        int k = 2;

        //Create file with RANDOM centroids
//        dataGenerator.writeDatasetToCSV(k, 10000, "src/main/data/centroids.csv", false);

        //Create file with centroids from data points
        dataGenerator.writeDatasetToCSV(k, 10000, "src/main/data/centroids.csv", true);

        //Start map-reduce job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "kMeans");

        job.setJarByClass(kMeansSingle.class);
        job.setMapperClass(kMeansSingle.Map.class);
        job.setReducerClass(kMeansSingle.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Configure the DistributedCache
        DistributedCache.addCacheFile(new Path("src/main/data/centroids.csv").toUri(), job.getConfiguration());
        DistributedCache.setLocalFiles(job.getConfiguration(), "src/main/data/centroids.csv");

        // Delete the output directory if it exists
        Path outputPath = new Path("src/main/data/kMeanOutput/centroids.csv");
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true); // true will delete recursively
        }

        FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project2/dataset.csv"));
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean ret = job.waitForCompletion(true);

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        System.exit(ret ? 0 : 1);

    }

}
