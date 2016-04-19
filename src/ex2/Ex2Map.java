package ex2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Ex2Map extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		int num = Integer.parseInt(ivalue.toString());

		if ((num % 2) == 0) {
			context.write(new Text("even"), new IntWritable(num));
		} else {
			context.write(new Text("odd"), new IntWritable(num));
		}
	}

}
