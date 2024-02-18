import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

//Single-iteration k-means clustering algorithm
public class kMeansOptimizedBonus {

    public static class Map extends Mapper<Object, Text, Text, Text> {

        private List<String> centroids = new ArrayList<>();

        //Add setup to read centroid file in memory
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);
            // open the stream
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            // wrap it into a BufferedReader object which is easy to read a record
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis,
                    "UTF-8"));
            // read the record line by line
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] split = line.split("\t");
                String[] split1 = split[0].split(",");
                centroids.add(split1[0] + "," + split1[1]);
            }
            // close the stream
            IOUtils.closeStream(reader);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");

            //Get point coordinates
            Double pointLong = Double.valueOf(values[0]);
            Double pointLat = Double.valueOf(values[1]);
            int pointAge = Integer.valueOf(values[2]);

            //Closest centroid
            double closestDistance = Double.POSITIVE_INFINITY;
            double[] closestCentroid = new double[]{0,0};

            //Go through each centroid and calculate distance
            for(int i = 0; i < centroids.size(); i++) {

                String[] centroid = centroids.get(i).split(",");

                double centroidLong = Double.valueOf(centroid[1]);
                double centroidLat = Double.valueOf(centroid[0]);

                double distance = Math.sqrt(Math.pow(pointLat-centroidLat,2) + Math.pow(Math.abs(pointLong)-Math.abs(centroidLong),2));

                if(distance < closestDistance) {
                    closestDistance = distance;
                    closestCentroid[0] = centroidLat;
                    closestCentroid[1] = centroidLong;
                }

            }
            Text outputK = new Text(closestCentroid[0] + "," + closestCentroid[1]);
            Text outputV = new Text(pointLat + "," + pointLong + "," + pointAge);

            //After we found centroid, output centroid coord as key and data point coord as value
            context.write(outputK, outputV);

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //Calculate new centroids
            double sumLat = 0;
            double sumLong = 0;
            int sumAge = 0;
            int count = 0;

            String points = "";

            for (Text x : values) {

                String[] split = x.toString().split(";");

                String[] point = split[0].toString().split(",");
                sumLat += Double.valueOf(point[0]);
                sumLong += Double.valueOf(point[1]);
                sumAge += Integer.valueOf(point[2]);
                count += Integer.valueOf(point[3]);

                points = split[1];

            }

            double centroidLat = sumLat / count;
            double centroidLong = sumLong / count;
            int centroidAge = sumAge / count;


            context.write(new Text(centroidLat + "," + centroidLong + "," + centroidAge), new Text(points));

        }

    }

    public static class Combiner extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder allPoints = new StringBuilder();

            //Calculate new centroids
            double sumLat = 0;
            double sumLong = 0;
            int sumAge = 0;
            int count = 0;

            for (Text x : values) {
                String[] point = x.toString().split(",");
                sumLat += Double.valueOf(point[0]);
                sumLong += Double.valueOf(point[1]);
                sumAge += Integer.valueOf(point[2]);
                count += 1;
                if(allPoints.length() > 0) {
                    allPoints.append(" | ");
                }
                allPoints.append(x);
            }


            context.write(key, new Text(sumLat + "," + sumLong + "," +  sumAge + "," + count + ";" + allPoints));

        }

    }

    public static List<String> getListFromFile(String fileName) {
        List<String> output = new ArrayList<>();
        File file = new File(fileName);
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                output.add(line);
            }
            IOUtils.closeStream(reader);
        } catch(FileNotFoundException e) {
            System.out.println("File not found");
        } catch(IOException e) {
            System.out.println(e);
        }
        return output;
    }

    //Pass K number/ input and output is fixed
    public static void main(String[] args) throws Exception{

        long startTime = System.currentTimeMillis();

        //Get number of centroids
//        int k = Integer.valueOf(args[0]);
        int k = 5;

        //Create file with RANDOM centroids
//        dataGenerator.writeDatasetToCSV(k, 10000, "src/main/data/centroids.csv", false);

        //Create file with centroids from data points
        dataGeneratorBonus.writeDatasetToCSV(k, 0, "src/main/data/centroids.csv", true);

        boolean ret = false;

        //Iterate 20 times or less
        int R = 20;
        for(int i = 0; i < R; i++) {

            //Start map-reduce job
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "kMeans");

            job.setJarByClass(kMeansOptimizedBonus.class);
            job.setMapperClass(kMeansOptimizedBonus.Map.class);
            job.setCombinerClass(kMeansOptimizedBonus.Combiner.class);
            job.setReducerClass(kMeansOptimizedBonus.Reduce.class);
            job.setNumReduceTasks(1);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            if(i == 0) {
                // Configure the DistributedCache
                job.addCacheFile(new URI("src/main/data/centroids.csv"));
            } else {
                job.addCacheFile(new URI("src/main/data/kMeanOutput/centroids" + (i-1) + ".csv/part-r-00000"));
            }

            // Delete the output directory if it exists
            Path outputPath = new Path("src/main/data/kMeanOutput/centroids" + i + ".csv");
            FileSystem fs = outputPath.getFileSystem(conf);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true); // true will delete recursively
            }

            FileInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/project2/homeData.csv"));
            FileOutputFormat.setOutputPath(job, outputPath);

            ret = job.waitForCompletion(true);

            boolean convergedAll = false;

            if(i > 0) {

                convergedAll = true;

                //Read current centroids
                List<String> currentC = getListFromFile("src/main/data/kMeanOutput/centroids" + i + ".csv/part-r-00000");

                //Read previous centroids
                List<String> previousC = getListFromFile("src/main/data/kMeanOutput/centroids" + (i - 1) + ".csv/part-r-00000");

                //See if there is a center that didn't converge
                for (String center : currentC) {
                    if (!previousC.contains(center)) {
                        convergedAll = false;
                        break;
                    }
                }

            }

            //Stop iterating when all centers converged
            if(convergedAll) break;

        }

        long endTime = System.currentTimeMillis();
        System.out.println((endTime - startTime) / 1000.0 + " seconds");

        System.exit(ret ? 0 : 1);

    }

}
