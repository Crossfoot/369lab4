package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class HadoopApp {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Hadoop example");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

	if (otherArgs.length < 3) {
	    System.out.println("Expected parameters: <job class> [<input dir>]+ <output dir>");
	    System.exit(-1);
	} else if ("CountryRequestCount".equalsIgnoreCase(otherArgs[0])) {

	    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, CountryRequestCount.CountryMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, CountryRequestCount.AccessLogMapper.class ); 

	    job.setReducerClass(CountryRequestCount.JoinReducer.class);

	    job.setOutputKeyClass(CountryRequestCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(CountryRequestCount.OUTPUT_VALUE_CLASS);

	    FileOutputFormat.setOutputPath(job, new Path("out_temp"));
		job.waitForCompletion(true);

		Job j2 = new Job(conf, "Hadoop example");

		j2.setReducerClass(CountrySum.ReducerImpl.class);
		j2.setMapperClass(CountrySum.MapperImpl.class);
		j2.setOutputKeyClass(CountrySum.OUTPUT_KEY_CLASS);
		j2.setOutputValueClass(CountrySum.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(j2, new Path("out_temp"));
		FileOutputFormat.setOutputPath(j2, new Path("out_sort"));

		j2.waitForCompletion(true);

		Job j3 = new Job(conf, "Hadoop example");

		j3.setReducerClass(Sorter.ReducerImpl.class);
		j3.setMapperClass(Sorter.MapperImpl.class);
		j3.setOutputKeyClass(Sorter.OUTPUT_KEY_CLASS);
		j3.setOutputValueClass(Sorter.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(j3, new Path("out_sort"));
		FileOutputFormat.setOutputPath(j3, new Path(otherArgs[3]));

		System.exit(j3.waitForCompletion(true) ? 0 : 1);

	} else if ("CountryUrlCount".equalsIgnoreCase(otherArgs[0])) {

	    conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");

	    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
					KeyValueTextInputFormat.class, CountryUrlCount.CountryMapper.class );
	    MultipleInputs.addInputPath(job, new Path(otherArgs[2]),
					TextInputFormat.class, CountryUrlCount.AccessLogMapper.class ); 

	    job.setReducerClass(CountryUrlCount.JoinReducer.class);

	    job.setOutputKeyClass(CountryUrlCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(CountryUrlCount.OUTPUT_VALUE_CLASS);

	    FileOutputFormat.setOutputPath(job, new Path("out_temp"));
		job.waitForCompletion(true);

		Job j2 = new Job(conf, "Hadoop example");

		j2.setReducerClass(UrlCount.ReducerImpl.class);
		j2.setMapperClass(UrlCount.MapperImpl.class);
		j2.setOutputKeyClass(UrlCount.OUTPUT_KEY_CLASS);
		j2.setOutputValueClass(UrlCount.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(j2, new Path("out_temp"));
		FileOutputFormat.setOutputPath(j2, new Path("out_sort"));

		j2.waitForCompletion(true);

		Job j3 = new Job(conf, "Hadoop example");

		j3.setReducerClass(CountryUrlSort.ReducerImpl.class);
		j3.setMapperClass(CountryUrlSort.MapperImpl.class);
		j3.setPartitionerClass(CountryUrlSort.PartitionerImpl.class);
		j3.setGroupingComparatorClass(CountryUrlSort.GroupingComparator.class);
		j3.setSortComparatorClass(CountryUrlSort.SortComparator.class);

		j3.setOutputKeyClass(CountryUrlSort.OUTPUT_KEY_CLASS);
		j3.setOutputValueClass(CountryUrlSort.OUTPUT_VALUE_CLASS);
		FileInputFormat.addInputPath(j3, new Path("out_sort"));
		FileOutputFormat.setOutputPath(j3, new Path(otherArgs[3]));

		System.exit(j3.waitForCompletion(true) ? 0 : 1);

	} else if ("WordCount".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(WordCount.ReducerImpl.class);
	    job.setMapperClass(WordCount.MapperImpl.class);
	    job.setOutputKeyClass(WordCount.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(WordCount.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else if ("AccessLog".equalsIgnoreCase(otherArgs[0])) {
	    job.setReducerClass(AccessLog.ReducerImpl.class);
	    job.setMapperClass(AccessLog.MapperImpl.class);
	    job.setOutputKeyClass(AccessLog.OUTPUT_KEY_CLASS);
	    job.setOutputValueClass(AccessLog.OUTPUT_VALUE_CLASS);
	    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	} else {
	    System.out.println("Unrecognized job: " + otherArgs[0]);
	    System.exit(-1);
	}
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }

}
