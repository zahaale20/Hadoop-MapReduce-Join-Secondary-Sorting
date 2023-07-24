package csc369;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountryCount2 {

	public static final Class OUTPUT_KEY_CLASS = Text.class;
	public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

	// Mapper for User file
	public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] str = value.toString().split("\t");
			String country = str[0];
			int count = Integer.parseInt(str[1]);
			context.write(new Text(country), new IntWritable(count));
		}
	}

	public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {

		private Map<Text, LongWritable> map = new HashMap<>();

		@Override
		protected void reduce(Text country, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable count : counts) {
				sum += count.get();
			}
			map.put(new Text(country), new LongWritable(sum));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			List<Map.Entry<Text, LongWritable>> list = new ArrayList<>(map.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<Text, LongWritable>>() {
				public int compare(Map.Entry<Text, LongWritable> value1, Map.Entry<Text, LongWritable> value2) {
					return Long.compare(value2.getValue().get(), value1.getValue().get());
				}
			});

			for (Map.Entry<Text, LongWritable> entry : list) {
				context.write(entry.getKey(), new IntWritable((int) entry.getValue().get()));
			}
		}
	}


	public static class DescendingIntWritableComparator extends WritableComparator {
		protected DescendingIntWritableComparator() {
			super(IntWritable.class, true);
		}

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			IntWritable aCount = (IntWritable) a;
			IntWritable bCount = (IntWritable) b;

			// Reverse the order of comparison to sort in descending order
			return aCount.compareTo(bCount);
		}
	}


}