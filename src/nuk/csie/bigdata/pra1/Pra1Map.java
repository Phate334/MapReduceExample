package nuk.csie.bigdata.pra1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Pra1Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	private IntWritable one = new IntWritable(1);

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {
		String[] words = ivalue.toString().split("[^\\w]");
		for (String s : words) {
			if (s.length() > 2) {
				context.write(new Text(s), one);
			}
		}
	}

}
