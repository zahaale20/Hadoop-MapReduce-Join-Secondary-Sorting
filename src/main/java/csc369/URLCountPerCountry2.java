package csc369;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class URLCountPerCountry2 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    // Mapper for User file
    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split("\t");
            context.write(new Text(str[0] + "\t" + str[1]), new IntWritable(1));
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Map<Text, LongWritable> map = new HashMap<>();

        @Override
        protected void reduce(Text URL, Iterable<IntWritable> counts, Context context) throws IOException, InterruptedException {
            int sum = 0;

            for (IntWritable count : counts) {
                sum += count.get();
            }
            map.put(new Text(URL), new LongWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<Text, LongWritable>> list = new ArrayList<>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<Text, LongWritable>>() {
                public int compare(Map.Entry<Text, LongWritable> value1, Map.Entry<Text, LongWritable> value2) {
                    int countryComparison = value1.getKey().toString().split("\t")[0].compareTo(value2.getKey().toString().split("\t")[0]);
                    if (countryComparison == 0) {
                        // Sort by count in descending order
                        return -Long.compare(value1.getValue().get(), value2.getValue().get());
                    }
                    return countryComparison;
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