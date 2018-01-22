
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class affinMap extends MapReduceBase 
implements Mapper<LongWritable, Text, Text, Text> {

	 private static final IntWritable one = new IntWritable(1);
	   
	public void map(LongWritable fileOffset, Text lineContents,
		      OutputCollector<Text, Text> output, Reporter reporter)
		      throws IOException {
		    String content[] = lineContents.toString().split("\\s+");
		 //   System.out.println(content[1]);	
		    StringBuffer data = new StringBuffer();
		    int score = Integer.parseInt(content[content.length-1]);
		    if (score < -3)
		    	content[content.length-1]="1";
		    else if (score >= -3 && score <= -1)
		    	content[content.length-1]="2";
		    else if (score >= -1 && score <= 1)
		    	content[content.length-1]="3";
		    else if (score >= 1 && score <= 3)
		    	content[content.length-1]="4";
		    else if (score >= 3 && score < 5)
		    	content[content.length-1]="5";
		    for (int i=0;i<content.length;i++){
		    	data = data.append(content[i]+" ");
		    }
		    output.collect(new Text(content[0]), new Text(data.toString()));
		  }


}
