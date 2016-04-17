package nuk.csie.bigdata.ex2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Ex2Reduce extends
		Reducer<Text, IntWritable, NullWritable, IntWritable> {

	private MultipleOutputs<NullWritable, IntWritable> out;

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
		// 在這個例子中只會出現兩種Key: even、odd
		String outPath = _key.toString();
		for (IntWritable val : values) {
			out.write(NullWritable.get(), val, outPath); // 依照key來決定收到的值要放到哪一個檔案中。
		}
	}

}
