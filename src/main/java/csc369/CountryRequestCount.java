package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryRequestCount {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for country file
    public static class CountryMapper extends Mapper<Text, Text, Text, Text> {
		private Text ip = new Text();
		private Text country = new Text();

		@Override
		public void map(Text key, Text value, Context context)  throws IOException, InterruptedException {
			String[] tokens = key.toString().split(",");
			ip.set(tokens[0]);
			country.set("A\t"+tokens[1]);
			context.write(ip, country);
		} 
    }

    // Mapper for access log file
    public static class AccessLogMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text ip = new Text();
		private Text one = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String text[] = value.toString().split(" ");
			ip.set(text[0]);
			one.set("B\t1");
			context.write(ip, one);
		}
    }


    //  Reducer: just one reducer class to perform the "join"
    public static class JoinReducer extends Reducer<Text, Text, Text, IntWritable> {
		private Text country = new Text();
		private IntWritable sum = new IntWritable();

		@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
			int running = 0;

			country.set("Never set");

			for (Text val : values) {
				// System.out.println("val: " + val.toString());
				if (val.toString().charAt(0) == 'A') {
					country.set(val.toString().substring(2));
				} else {
					running += Integer.parseInt(val.toString().substring(2));
				}
			}
			sum.set(running);
			context.write(country, sum);
		}
    } 


}
