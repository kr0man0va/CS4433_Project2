import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class dataGeneratorBonus {

    public static void main(String[] args) throws Exception{
        //Create data file with points

        //Change to 3000
        int dataSize = 3000;
        int maxValueXY = 5000;

        writeDatasetToCSV(dataSize, maxValueXY, "src/main/data/dataset.csv", false);
    }

    public static void writeDatasetToCSV(int size, int maxValue, String filename, boolean centroids) {
        Configuration conf = new Configuration();
        double minLongitude = -122.30;
        double maxLongitude = -122.20;
        double minLatitude = 37.7;
        double maxLatitude = 37.82;

        try (FileSystem fs = FileSystem.get(conf)) {
            Path hdfsPath = new Path(filename);
            FSDataOutputStream outputStream = fs.create(hdfsPath);

            PrintWriter writer = new PrintWriter(new OutputStreamWriter(outputStream));

            Random random = new Random();

            if(centroids) {
                for (int i = 0; i < size; i++) {
                    List<String> points = getListFromFile("src/main/data/dataset.csv");
                    int index = random.nextInt(points.size());
                    writer.println(points.get(index));
                }
            } else {
                // Write data
                for (int i = 0; i < size; i++) {
                    double randomLongitude = minLongitude + (maxLongitude - minLongitude) * random.nextDouble();
                    double randomLatitude = minLatitude + (maxLatitude - minLatitude) * random.nextDouble();
                    writer.println(randomLatitude + "," + randomLongitude);
                }
            }

            writer.close();
            outputStream.close();

        } catch (IOException e) {
            System.out.println("CSV file failed.");
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

}
