import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SplitUSDataMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");
        StringBuilder splitLine = new StringBuilder();

        for (String column : columns) {
            splitLine.append(column).append(",");
        }

        // Remove the last comma
        if (splitLine.length() > 0) {
            splitLine.setLength(splitLine.length() - 1);
        }

        context.write(key, new Text(splitLine.toString()));
    }
}
