package homework;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HwMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		IntWritable[] argTagIndex = DefaultStringifier.loadArray(conf, "args", IntWritable.class); // 前面輸入的參數，要做GroupBy的欄位index。

		try {
			String[] row = ivalue.toString().trim().split(",");
			int cnt = Integer.parseInt(row[row.length - 1]); // 取出cnt欄的值，第一行沒辦法轉數字會跳例外。

			String[] key = new String[argTagIndex.length];
			for (int i = 0; i < argTagIndex.length; i++) // 要作GroupBy的欄位index
			{
				int outDataIndex = argTagIndex[i].get();

				if (outDataIndex >= 10 && outDataIndex <= 13) // 溫度temp、體感溫度atemp、濕度hum、風速windspeed
																// 離散化
				{
					Float contin = new Float(row[outDataIndex]) * 10;
					key[i] = String.valueOf(contin.intValue());
				} else {
					key[i] = row[outDataIndex];
				}
			}

			context.write(new Text(String.join("\t", key)), new IntWritable(cnt));
		} catch (NumberFormatException e) {
			// 第一行是欄位名字，讀到這行時會因為最後一欄cnt無法轉數字而跳過這行。
			// 不需要處理
		}
	}

}
