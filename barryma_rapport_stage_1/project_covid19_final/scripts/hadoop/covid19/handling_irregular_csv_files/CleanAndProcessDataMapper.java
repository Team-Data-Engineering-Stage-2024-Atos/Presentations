import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanAndProcessDataMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    private static final int COMMON_COLUMN_COUNT = 1147; // Update this value as needed

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(",");
        StringBuilder cleanedLine = new StringBuilder();

        for (int i = 0; i < COMMON_COLUMN_COUNT; i++) {
            if (i < columns.length) {
                cleanedLine.append(columns[i]);
            } else {
                cleanedLine.append("");
            }
            if (i < COMMON_COLUMN_COUNT - 1) {
                cleanedLine.append(",");
            }
        }

        context.write(key, new Text(cleanedLine.toString()));
    }
}
