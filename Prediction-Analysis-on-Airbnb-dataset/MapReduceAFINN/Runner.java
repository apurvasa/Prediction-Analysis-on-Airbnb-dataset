
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;


public class Runner {
	public static void main(String[] args) throws Exception
	  {
	        JobConf conf = new JobConf(Runner.class);
	        conf.setJobName("affin");
	        
	        conf.setMapperClass(affinMap.class);
	        
	        conf.setMapOutputKeyClass(Text.class);
	        conf.setMapOutputValueClass(Text.class);
	        
	        conf.setReducerClass(affinRed.class);
	        
	        
	        // take the input and output from the command line
	        FileInputFormat.setInputPaths(conf, new Path(args[0]));
	        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	        JobClient.runJob(conf); 

		}

}

