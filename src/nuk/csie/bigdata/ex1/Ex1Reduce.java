package nuk.csie.bigdata.ex1;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Ex1Reduce extends Reducer<IntWritable, IntWritable, NullWritable, IntWritable> {

	public void reduce(IntWritable _key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		int counter = 0;
		
		for (IntWritable val : values) {
			counter += val.get();
		}
		
		context.write(NullWritable.get(), new IntWritable(counter));
	}

}
