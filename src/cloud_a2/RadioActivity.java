package cloud_a2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RadioActivity extends Configured implements Tool {
	public static class Map extends Mapper<LongWritable, Text, Text, FloatWritable> {
//		Log log = LogFactory.getLog(Map.class);
		String locationProvinceYear;
		private float cs134Activity;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			if (line.contains("Location")) {
				return;
			}
			String[] splitted = line.split(",");
			locationProvinceYear = splitted[0] + "-" + splitted[1] + "-" + splitted[2].substring(0, 3);
			cs134Activity = Float.valueOf(splitted[11]);	//this is dumb and will not return a lot of results
			context.write(new Text(locationProvinceYear), new FloatWritable(cs134Activity));
		}
	}
	
	public static class Reduce extends Reducer<Text, FloatWritable, Text, Text> {
		public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			float min = 0;
			float max = 0;
			float total = 0;
			float denum = 0;
			float avg;
			for (FloatWritable val:values) {
				if (val.get() < min) {
					min = val.get();
				}
				if (val.get() > max) {
					max = val.get();
				}
				total += val.get();
				denum ++;
			}
			avg = total/denum;
			Text output = new Text(max + "\t" + min + "\t" + avg);
			context.write(key, output);
		}
	}
	
	public int run(String[] args) throws IOException {
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("RadioActivityExtraction");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass((Class<? extends org.apache.hadoop.mapred.Mapper>) Map.class);
		conf.setReducerClass((Class<? extends org.apache.hadoop.mapred.Reducer>) Reduce.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		JobClient job = new JobClient(conf);
		RunningJob runJob = job.submitJob(conf);
		runJob.waitForCompletion();	//important. Don't return until completed
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Please enter a location, province and year to extract the activity information.");
		System.out.println("Example: Calgary-AB-2009");
		String key = System.console().readLine();
		//run program
		int res = ToolRunner.run(new Configuration(), new RadioActivity(), args);

		//get output file and extract the line with specified key
		String outputFile = args[1].endsWith("/") ? args[1] + "part-00000" : args[1] + "/part-00000";
		try(BufferedReader br = new BufferedReader(new FileReader(outputFile))) {
			for (String line; (line = br.readLine()) != null;) {
				if (line.startsWith(key)) {
					System.out.println(line);
					break;
				}
			}
		}
		System.exit(res);
	}
}
