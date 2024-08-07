import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USConfirmedCasesMonthlyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text stateMonthYear = new Text();
    private LongWritable cases = new LongWritable();
    private String[] headers;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String headerLine = context.getConfiguration().get("headerLine");
        if (headerLine != null) {
            headers = headerLine.split(",", -1);
        } else {
            throw new IOException("Header line is missing in the configuration");
        }
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",", -1);

        if (fields[0].equals("UID")) {
            return; // Skip header row
        }

        String state = fields[6];
        for (int i = 11; i < fields.length; i++) {
            if (fields[i] != null && !fields[i].isEmpty()) {
                try {
                    long caseCount = Long.parseLong(fields[i]);
                    String[] dateParts = headers[i].split("/");
                    if (dateParts.length < 3) {
                        // Log the issue and continue
                        System.err.println("Invalid date format in header: " + headers[i]);
                        continue;
                    }
                    String monthYear = "Month_" + dateParts[0] + "_Year_" + dateParts[2];
                    stateMonthYear.set(state + ",Total_Cases_" + monthYear);
                    cases.set(caseCount);
                    context.write(stateMonthYear, cases);
                } catch (NumberFormatException e) {
                    // Log the issue and continue
                    System.err.println("Invalid number format: " + fields[i]);
                }
            }
        }
    }
}
