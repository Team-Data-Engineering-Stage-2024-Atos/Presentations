import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USConfirmedCasesYearlyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text stateYear = new Text();
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

        // Skip the header row
        if (fields[0].equals("Province_State")) {
            return;
        }

        String state = fields[0]; // Province_State field
        Map<String, Long> yearlyCases = new HashMap<>();

        for (int i = 11; i < fields.length; i++) {
            if (i >= headers.length) {
                System.err.println("Index out of bounds for headers array: " + i);
                continue; // Skip if the index is out of bounds for headers
            }

            if (fields[i] != null && !fields[i].isEmpty()) {
                try {
                    long caseCount = Long.parseLong(fields[i]);
                    String[] dateParts = headers[i].split("/");
                    if (dateParts.length < 3) {
                        // Log the issue and continue
                        System.err.println("Invalid date format in header: " + headers[i]);
                        continue;
                    }
                    String year = dateParts[2];
                    yearlyCases.put(year, yearlyCases.getOrDefault(year, 0L) + caseCount);
                } catch (NumberFormatException e) {
                    // Log the issue and continue
                    System.err.println("Invalid number format: " + fields[i]);
                }
            }
        }

        for (Map.Entry<String, Long> entry : yearlyCases.entrySet()) {
            stateYear.set(state + ",Total_Cases_Year_" + entry.getKey());
            cases.set(entry.getValue());
            context.write(stateYear, cases);
        }
    }
}
