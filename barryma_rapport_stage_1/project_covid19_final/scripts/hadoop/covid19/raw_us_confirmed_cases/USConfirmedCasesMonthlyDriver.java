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

public class USConfirmedCasesMonthlyDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: USConfirmedCasesMonthlyDriver <input path> <output path>");
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

        Job job = Job.getInstance(conf, "US Confirmed Cases Monthly");
        job.setJarByClass(USConfirmedCasesMonthlyDriver.class);
        job.setMapperClass(USConfirmedCasesMonthlyMapper.class);
        job.setCombinerClass(USConfirmedCasesMonthlyReducer.class);
        job.setReducerClass(USConfirmedCasesMonthlyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
