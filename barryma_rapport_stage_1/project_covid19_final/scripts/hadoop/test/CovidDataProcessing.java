import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CovidDataProcessing {

    public static class CovidMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();
        private SimpleDateFormat inputFormat = new SimpleDateFormat("MM/dd/yyyy");
        private SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");
            
            if (fields.length < 5) return;  // Skip invalid records
            
            try {
                Date date = inputFormat.parse(fields[1]);
                String formattedDate = outputFormat.format(date);
                
                outputKey.set(fields[0] + "," + formattedDate);  // Combine state and date as key
                outputValue.set(fields[2] + "," + fields[3] + "," + fields[4]);  // Confirmed cases, deaths, population
                context.write(outputKey, outputValue);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class CovidReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Covid Data Processing");
        job.setJarByClass(CovidDataProcessing.class);
        job.setMapperClass(CovidMapper.class);
        job.setReducerClass(CovidReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
