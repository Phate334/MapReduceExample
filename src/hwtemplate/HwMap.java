package hwtemplate;

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

			/* 將每一行裡面需要的資料從取出來。
			 * 1. 如果是連續的資料型態例如 溫度、體感溫度、濕度、風速 ，該如何處理??
			 * 2. 該怎麼設計Mapper輸出的KEY，以便將資料聚集起來??
			 * 3. 如果要GroupBy的欄位是兩個以上怎麼做??
			 */
		} catch (NumberFormatException e) {
			// 第一行是欄位名字，讀到這行時會因為最後一欄cnt無法轉數字而跳過這行。
			// 不需要處理
		}
	}

}
