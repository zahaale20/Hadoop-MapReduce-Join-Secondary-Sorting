package csc369;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryCount {

	public static final Class OUTPUT_KEY_CLASS = Text.class;
	public static final Class OUTPUT_VALUE_CLASS = Text.class;

	// Mapper for User file
	public static class HostCountryMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			String country = value.toString();
			String out = "A\t" + country;
			context.write(key, new Text(out));
		}
	}

	// Mapper for messages file
	public static class LogMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String text[] = value.toString().split(" ");
			String hostname = text[0];
			String out = "B\t" + 1;
			context.write(new Text(hostname), new Text(out));
		}
	}

	public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String country = "";
			int count = 0;

			for (Text val : values) {
				String[] temp = val.toString().split("\t");
				if (temp[0].equals("A")) {
					country = temp[1];
				} else if (temp[0].equals("B")) {
					count += Integer.parseInt(temp[1]);
				}
			}
			context.write(new Text(country), new Text(count + ""));
		}
	}
}