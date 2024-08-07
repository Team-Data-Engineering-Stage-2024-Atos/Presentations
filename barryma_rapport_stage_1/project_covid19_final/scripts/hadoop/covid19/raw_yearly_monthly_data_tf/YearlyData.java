import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class YearlyData {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: YearlyData <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path inputPath = new Path(args[0]);
        String headerLine;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(inputPath)))) {
            headerLine = br.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
            return; // Ensure the method terminates after exit
        }

        conf.set("headerLine", headerLine);

        Job job = Job.getInstance(conf, "Yearly Data");
        job.setJarByClass(YearlyData.class);
        job.setMapperClass(YearlyMapper.class);
        job.setCombinerClass(YearlyReducer.class);
        job.setReducerClass(YearlyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
