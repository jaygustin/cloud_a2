import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Map implements Mapper<LongWritable, Text, Text, FloatWritable> {
	// Log log = LogFactory.getLog(Map.class);
	String yearLocation;
	private float cs134Activity;

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, FloatWritable> output, Reporter arg3)
			throws IOException {
		String line = value.toString();
		//if this line is the header line, don't process it
		if (line.contains("Location")) {
			return;
		}
		String[] splitted = line.split(",", -1);
		//build the key
		yearLocation = splitted[2].substring(0, 4) + "-" + splitted[0];
		//get the CDM column of Cs134. If it is empty, set value to 0
		cs134Activity = (splitted[13].isEmpty()) ? 0 : Float.valueOf(splitted[13]);
		output.collect(new Text(yearLocation), new FloatWritable(cs134Activity));
	}
}
