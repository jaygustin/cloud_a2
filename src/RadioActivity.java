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
		String[] arg = {"nms_airborne_radioactivity_ssn_radioactivite_dans_air.csv","output"};
		Scanner s = new Scanner(System.in);
		System.out.println("Please enter a year");
		String year = s.nextLine();
		System.out.println("Please enter a location");
		String location = s.nextLine();
		String key = year + "-" + location;
		
		// run program
		int res = ToolRunner.run(new Configuration(), new RadioActivity(), arg);
		// get output file and extract the line with specified key
		try (BufferedReader br = new BufferedReader(new FileReader("output/part-00000"))) {
			boolean found = false;
			for (String line; (line = br.readLine()) != null;) {
				if (line.startsWith(key)) {
					System.out.println("year-location max    min    avg");
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
