package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Sorter {

    public static final Class OUTPUT_KEY_CLASS = IntWritable.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, IntWritable, Text> {
	private final IntWritable one = new IntWritable(1);
	private Text word = new Text();

        @Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split("\t");
            context.write(new IntWritable(-Integer.parseInt(str[1])), new Text(str[0]));
        }
    }

    public static class ReducerImpl extends Reducer<IntWritable, Text, IntWritable, Text> {
	private IntWritable result = new IntWritable();
    
        @Override
	protected void reduce(IntWritable count, Iterable<Text> requests, Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = requests.iterator();
            count = new IntWritable(-count.get());
        
            while (itr.hasNext()) {
                context.write(count, itr.next());
            }
       }
    }

}
