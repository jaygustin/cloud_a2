package cloud_a2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapOutputCollector.Context;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Reduce implements Reducer<Text, FloatWritable, Text, Text> {
	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {

	}

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reduce(Text key, Iterator<FloatWritable> values, OutputCollector<Text, Text> output, Reporter arg3)
			throws IOException {
		float min = 0;
		float max = 0;
		float total = 0;
		float denum = 0;
		float avg;
		while (values.hasNext()) {
			FloatWritable val = values.next();
			if (val.get() < min) {
				min = val.get();
			}
			if (val.get() > max) {
				max = val.get();
			}
			total += val.get();
			denum++;
		}
		avg = total / denum;
		Text out = new Text(max + "\t" + min + "\t" + avg);
		output.collect(key, out);
	}
}
