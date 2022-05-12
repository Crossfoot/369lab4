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

public class CountryUrlCount {

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
			country.set("A\t"+ tokens[1]);
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
			one.set("B\t1" + "\t" + text[6]);
			context.write(ip, one);
		}
    }


    private static class LinkCountPair {
        private String link;
        private Integer count;

        public LinkCountPair(String link, int count) {
            this.link = link;
            this.count = count;
        }

        public String getLink() {
            return link;
        }

        public Integer getCount() {
            return count;
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
		private Text country = new Text();

		@Override
	    public void reduce(Text key, Iterable<Text> values, Context context)  throws IOException, InterruptedException {
			country.set("Never set");
            ArrayList<LinkCountPair> lc_pairs = new ArrayList<LinkCountPair>();

			for (Text val : values) {
				if (val.toString().charAt(0) == 'A') {
					country.set(val.toString().substring(2));
				} else {
                    String[] str = val.toString().split("\t");
                    lc_pairs.add(new LinkCountPair(str[2], Integer.parseInt(str[1])));
				}
			}
            for (LinkCountPair lcp : lc_pairs) {
                context.write(country, new Text(lcp.getCount() + "\t" + lcp.getLink()));
            }
		}
    } 


}
