package csc369;

import java.io.IOException;

import org.apache.hadoop.io.Text;
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
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");

		Job job = new Job(conf, "Hadoop example");
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length < 3) {
			System.out.println("Expected parameters: <job class> <input dir> <output dir>");
			System.exit(-1);
		} else if ("1.1".equalsIgnoreCase(otherArgs[0])) {

			MultipleInputs.addInputPath(job, new Path(otherArgs[1]), KeyValueTextInputFormat.class, CountryCount.HostCountryMapper.class );
			MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, CountryCount.LogMapper.class );

			job.setReducerClass(CountryCount.JoinReducer.class);

			job.setOutputKeyClass(CountryCount.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(CountryCount.OUTPUT_VALUE_CLASS);
			FileInputFormat.setInputPaths(job, new Path(otherArgs[1]), new Path(otherArgs[2]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		} else if ("1.2".equalsIgnoreCase(otherArgs[0])) {
			job.setMapperClass(CountryCount2.MapperImpl.class);
			job.setReducerClass(CountryCount2.ReducerImpl.class);
			job.setSortComparatorClass(CountryCount2.DescendingIntWritableComparator.class);
			job.setOutputKeyClass(CountryCount2.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(CountryCount2.OUTPUT_VALUE_CLASS);
			FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		} else if ("2.1".equalsIgnoreCase(otherArgs[0])) {

			MultipleInputs.addInputPath(job, new Path(otherArgs[1]), KeyValueTextInputFormat.class, URLCountPerCountry.HostCountryMapper.class );
			MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, URLCountPerCountry.LogMapper.class );
			job.setReducerClass(URLCountPerCountry.JoinReducer.class);
			job.setOutputKeyClass(URLCountPerCountry.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(URLCountPerCountry.OUTPUT_VALUE_CLASS);
			FileInputFormat.setInputPaths(job, new Path(otherArgs[1]), new Path(otherArgs[2]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		} else if ("2.2".equalsIgnoreCase(otherArgs[0])) {
			job.setMapperClass(URLCountPerCountry2.MapperImpl.class);
			job.setReducerClass(URLCountPerCountry2.ReducerImpl.class);
			job.setSortComparatorClass(URLCountPerCountry2.DescendingIntWritableComparator.class);
			job.setOutputKeyClass(URLCountPerCountry2.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(URLCountPerCountry2.OUTPUT_VALUE_CLASS);
			FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		} else if ("3.1".equalsIgnoreCase(otherArgs[0])) {

			MultipleInputs.addInputPath(job, new Path(otherArgs[1]), KeyValueTextInputFormat.class, CountriesPerURL.HostCountryMapper.class );
			MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, CountriesPerURL.LogMapper.class );
			job.setReducerClass(CountriesPerURL.JoinReducer.class);
			job.setOutputKeyClass(CountriesPerURL.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(CountriesPerURL.OUTPUT_VALUE_CLASS);
			FileInputFormat.setInputPaths(job, new Path(otherArgs[1]), new Path(otherArgs[2]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

		} else if ("3.2".equalsIgnoreCase(otherArgs[0])) {
			job.setMapperClass(CountriesPerURL2.MapperImpl.class);
			job.setReducerClass(CountriesPerURL2.ReducerImpl.class);
			job.setOutputKeyClass(CountriesPerURL2.OUTPUT_KEY_CLASS);
			job.setOutputValueClass(CountriesPerURL2.OUTPUT_VALUE_CLASS);
			FileInputFormat.setInputPaths(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		} else {
			System.out.println("Unrecognized job: " + otherArgs[0]);
			System.exit(-1);
		}
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}

}
