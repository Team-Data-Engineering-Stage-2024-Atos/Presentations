import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class USDeathsYearlyDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: USDeathsYearlyDriver <input path> <output path> <header line>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("headerLine", args[2]);

        Job job = Job.getInstance(conf, "US Deaths Yearly Aggregation");
        job.setJarByClass(USDeathsYearlyDriver.class);
        job.setMapperClass(USDeathsYearlyMapper.class);
        job.setReducerClass(USDeathsYearlyReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
