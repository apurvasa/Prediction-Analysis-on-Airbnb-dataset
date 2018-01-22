

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class affinRed extends MapReduceBase implements Reducer<Text, Text, Text, Text> 
{
	public void reduce(Text numberconstant, Iterator<Text> counts,
		      OutputCollector<Text, Text> output, Reporter reporter)
		      throws IOException {
		   
		    double totalCount = 0;
		    
		    // loop over the count and tally it up
		   // String ipFinal = ip.toString()+",";
		    String content[] = new String[2];
		    content[0] = new String();
		    content[1] = new String();
		    while (counts.hasNext())
		    {
		    	Text count = counts.next();
		    	System.out.println(count);
		    	content = count.toString().split("\\s+");
		   //   totalCount += count.get();
		   //   System.out.println("count "+totalCount);
		    }
		  //  totalCount = totalCount*4.0;
		    
		    output.collect(new Text(content[0]), new Text(content[1]));
		  }
}
