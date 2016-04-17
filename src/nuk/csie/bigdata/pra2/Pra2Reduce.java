package nuk.csie.bigdata.pra2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * 策略一 Reducer:依照收到的KEY第一個字母決定輸出目的。
 */
public class Pra2Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

	private MultipleOutputs<Text, IntWritable> out;

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		out.close();
		super.cleanup(context);
	}

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		out = new MultipleOutputs<>(context);
		super.setup(context);
	}

	public void reduce(Text _key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		if (sum > 2) {
			String first = _key.toString().substring(0, 1);
			out.write(_key, new IntWritable(sum), first.toLowerCase());
		}
	}

}
