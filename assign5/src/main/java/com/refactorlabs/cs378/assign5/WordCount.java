package com.refactorlabs.cs378.assign5;

/**
 * Created by vinhnguyen on 10/2/16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


/**
 * Modified Word count program - a MapReduce program that performs word count
 */
public class WordCount {

    /**
     * Each count output from the map() function is "1", so to minimize small
     * object creation we can use a constant for this output value/object.
     */
    public final static LongWritable ONE = new LongWritable(1L);

    private final static String[] fieldNames = {"user_id", "event_type", "page", "referring_domain",
            "event_timestamp", "city", "vin", "vehicle_condition", "year", "make", "model", "trim",
            "body_style", "cab_style", "price", "mileage", "image_count", "free_carfax_report", "features"};

    /**
     * The Map class for word count.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word count example.
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        /**
         * Local variable "word" will contain the word identified in the input.
         */
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");

            context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

            // For each word in the input line, emit a count of 1 for that word.
            for (int i = 0; i < tokens.length; i++) {
                // ignore tokens at specific values
                if (i == 0 || i == 3 || i == 4 || i == 6 || i == 14 || i == 15 || i == 16) {
                    continue;
                }

                word.set(fieldNames[i] + ":" + tokens[i]);
                context.write(word, ONE);
                context.getCounter(MAPPER_COUNTER_GROUP, "Words Out").increment(1L);
            }
        }
    }


    /**
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
    public static class ReduceClass extends Reducer<Text, LongWritable, Text, LongWritable> {

        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0L;

            context.getCounter(REDUCER_COUNTER_GROUP, "Words Out").increment(1L);

            // Sum up the counts for the current word, specified in object "key".
            for (LongWritable value : values) {
                sum += value.get();
            }
            // Emit the total count for the word.
            context.write(key, new LongWritable(sum));
        }
    }


    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "WordCount");
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordCount.class);

        // Set the output key and value types (for map and reduce).
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // Set the map and reduce classes.
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setCombinerClass(ReduceClass.class);

        // Set the input and output file formats.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPath(job, new Path(appArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);
    }
}
