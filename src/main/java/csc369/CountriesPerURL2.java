package csc369;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountriesPerURL2 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = Text.class;

    // Mapper for User file
    public static class MapperImpl extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] str = value.toString().split("\t");
            Text url = new Text(str[0]);
            Text count = new Text(str[1]);
            context.write(url, count);
        }
    }

    public static class ReducerImpl extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text URL, Iterable<Text> countries, Context context) throws IOException, InterruptedException {
            TreeMap<String, Boolean> countryMap = new TreeMap<>();
            for (Text country : countries) {
                countryMap.put(country.toString(), true);
            }

            StringBuilder countryResult = new StringBuilder();
            for (String country : countryMap.keySet()) {
                countryResult.append(country).append(", ");
            }

            context.write(URL, new Text(countryResult.substring(0, countryResult.length() - 2)));
        }
    }
}