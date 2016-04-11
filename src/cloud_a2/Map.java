package cloud_a2;

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
	String locationProvinceYear;
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
		if (line.contains("Location")) {
			return;
		}
		String[] splitted = line.split(",");
		locationProvinceYear = splitted[0] + "-" + splitted[1] + "-" + splitted[2].substring(0, 3);
		cs134Activity = Float.valueOf(splitted[11]); // this is dumb and will
														// not return a lot of
														// results
		output.collect(new Text(locationProvinceYear), new FloatWritable(cs134Activity));
	}
}
