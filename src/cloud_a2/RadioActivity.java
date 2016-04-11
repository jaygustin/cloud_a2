package cloud_a2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RadioActivity extends Configured implements Tool {

	public int run(String[] args) throws IOException {
		System.out.println("before getClass");
		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("RadioActivityExtraction");
		System.out.println("before Text");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		System.out.println("before Map");
		conf.setMapperClass(Map.class);
		System.out.println("before Reduce");
		conf.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		JobClient job = new JobClient(conf);
		RunningJob runJob = job.submitJob(conf);
		runJob.waitForCompletion(); // important. Don't return until completed
		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Please enter a location, province and year to extract the activity information.");
		System.out.println("Example: Calgary-AB-2009");
		Scanner s = new Scanner(System.in);
		String key = s.nextLine();
		// run program
		int res = ToolRunner.run(new Configuration(), new RadioActivity(), args);

		// get output file and extract the line with specified key
		String outputFile = args[1].endsWith("/") ? args[1] + "part-00000" : args[1] + "/part-00000";
		try (BufferedReader br = new BufferedReader(new FileReader(outputFile))) {
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
