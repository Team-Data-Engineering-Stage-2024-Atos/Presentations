import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class USDeathsMonthlyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private Text stateMonth = new Text();
    private LongWritable deaths = new LongWritable();
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
        Map<String, Long> monthlyDeaths = new HashMap<>();

        for (int i = 12; i < fields.length; i++) { // Skip to the 13th column (index 12)
            if (i >= headers.length) {
                System.err.println("Index out of bounds for headers array: " + i);
                continue; // Skip if the index is out of bounds for headers
            }

            if (fields[i] != null && !fields[i].isEmpty()) {
                try {
                    long deathCount = Long.parseLong(fields[i]);
                    String[] dateParts = headers[i].split("/");
                    if (dateParts.length < 3) {
                        // Log the issue and continue
                        System.err.println("Invalid date format in header: " + headers[i]);
                        continue;
                    }
                    String monthYear = dateParts[0] + "_" + dateParts[2]; // e.g., "1_20" for January 2020
                    monthlyDeaths.put(monthYear, monthlyDeaths.getOrDefault(monthYear, 0L) + deathCount);
                } catch (NumberFormatException e) {
                    // Log the issue and continue
                    System.err.println("Invalid number format: " + fields[i]);
                }
            }
        }

        for (Map.Entry<String, Long> entry : monthlyDeaths.entrySet()) {
            stateMonth.set(state + ",Total_Deaths_Month_" + entry.getKey());
            deaths.set(entry.getValue());
            context.write(stateMonth, deaths);
        }
    }
}
