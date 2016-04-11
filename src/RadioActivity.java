import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
		JobConf conf = new JobConf(getConf(), getClass());
		//overall, we will get Text outputs
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		//The mapper output values are floats
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(FloatWritable.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient job = new JobClient(conf);
		RunningJob runJob = job.submitJob(conf);
		runJob.waitForCompletion();
		return 0;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("You must specify the input file and output directory when running this program.");
		}
		System.out.println("Please enter a location, province and year to extract the activity information.");
		System.out.println("Example: Calgary-AB-2009");
		Scanner s = new Scanner(System.in);
		String key = s.nextLine();
		
		if (!key.matches("([A-Z])([a-z])*-([A-Z])*-([0-9]){4}")) {
			System.out.println(key + ": Not a valid key, exiting program");
		}
		// run program
		int res = ToolRunner.run(new Configuration(), new RadioActivity(), args);
		// get output file and extract the line with specified key
		String outputFile = args[1].endsWith("/") ? args[1] + "part-00000" : args[1] + "/part-00000";
		try (BufferedReader br = new BufferedReader(new FileReader(outputFile))) {
			boolean found = false;
			for (String line; (line = br.readLine()) != null;) {
				if (line.startsWith(key)) {
					System.out.println("location-province-year max    min    avg");
					System.out.println(line);
					found = true;
					break;
				}
			}
			if (!found) {
				System.out.println("Could not find " + key + ", please try again with a different key.");
			}
		}
		s.close();
		System.exit(res);
	}
}
