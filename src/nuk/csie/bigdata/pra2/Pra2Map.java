package nuk.csie.bigdata.pra2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 策略一 Mapper:以單字當KEY，同Pra1。
 */
public class Pra2Map extends Mapper<LongWritable, Text, Text, IntWritable> {

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
