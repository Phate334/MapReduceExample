package ex1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Ex1Map extends
		Mapper<LongWritable, Text, IntWritable, IntWritable> {

	private IntWritable one = new IntWritable(1);

	public void map(LongWritable ikey, Text ivalue, Context context)
			throws IOException, InterruptedException {

		int num = Integer.parseInt(ivalue.toString());

		context.write(one, new IntWritable(num));
	}

}
