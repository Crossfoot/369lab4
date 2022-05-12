package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

public class CountryUrlSort {

    public static final Class OUTPUT_KEY_CLASS = CountryCountPair.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // read a text file that contains (y, m, d, temperature) readings
    public static class MapperImpl extends Mapper<LongWritable, Text, CountryCountPair, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");
            context.write(new CountryCountPair(tokens[0], -Integer.parseInt(tokens[2])), new Text(tokens[1]));
        }
    }
    

    // controls the reducer to which a particular (key, value) is sent
    public static class PartitionerImpl extends Partitioner<CountryCountPair, IntWritable> {
        @Override
        public int getPartition(CountryCountPair pair,
                                IntWritable temperature,
                                int numberOfPartitions) {
            return Math.abs(pair.getCountry().hashCode() % numberOfPartitions);
        }
    }
    
    // used to group (year,month,day) data by (year,month)
    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(CountryCountPair.class, true);
        }
        
        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            CountryCountPair pair = (CountryCountPair) wc1;
            CountryCountPair pair2 = (CountryCountPair) wc2;
            return pair.getCountry().compareTo(pair2.getCountry());
        }
    }

    // used to perform secondary sort on temperature
    public static class SortComparator extends WritableComparator {
        protected SortComparator() {
            super(CountryCountPair.class, true);
        }
        
        @Override
        public int compare(WritableComparable wc1,
                           WritableComparable wc2) {
            CountryCountPair pair = (CountryCountPair) wc1;
            CountryCountPair pair2 = (CountryCountPair) wc2;
            return pair.compareTo(pair2);
        }
    }
    
    // output one line for each month, with the temperatures sorted for that month
    public static class ReducerImpl extends Reducer<CountryCountPair, Text, Text, Text> {

            @Override
            protected void reduce(CountryCountPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                System.out.println("Here");

                for (Text value : values) {
                    Integer count = -key.getCount().get();
                    context.write(key.getCountry(), new Text(value.toString() + "\t" + count.toString()));
                }
            }
    }

}
