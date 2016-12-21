package com.refactorlabs.cs378.assign3;

// Imports
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.Arrays;
import java.util.HashMap;
//import org.apache.commons.lang3.StringUtils;
import java.lang.*;

import java.lang.*;

public class InvertedIndex extends Configured implements Tool {

    // Map and Reduce classes
	public static class MapClass extends Mapper<LongWritable, Text, Text, InvertedIndexWritable> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();

		/**
		 * HashMap to store each word and its frequency
		 */
		private static final Map<String, String> wordMap = new HashMap<String, String>();


		/**
		 * @param key
		 * @param value
		 * @param context
		 * @throws IOException
		 * @throws InterruptedException
		 * create map output
		 */

		private String[] punctuations = {"--", ",", ".", ";", ":", "?", "\\", "=", "_", "!", "(", ")", "\"", "[" , "]"};
		private String[] 	 no_punct = { " ",  "",  "",  "",  "",  "",   "",  "",  "",  "",  "",  "",   "", " [", "] "};

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// remove special characters
//			String line = org.apache.commons.lang.StringUtils.replaceEach(value.toString(), punctuations, no_punct);
			String line = value.toString();//.replaceAll("():;?[,.|\"'\t]", "");

			if (line.contains("--")) {
				line = line.replace("--", " ");
			}

			String[] words = line.split(" ");
			if (words.length != 0) {
				InvertedIndexWritable output = new InvertedIndexWritable();
				String verseIDList;
				String currentVerseID = words[0];
				System.out.println(currentVerseID);
				for (int i = 1; i < words.length; i++) {
					String w = words[i].toLowerCase().replaceAll("[^\\p{L}\\p{Nd}]+", "");
					if (wordMap.get(w) == null) {
						wordMap.put(w, currentVerseID);
					} else {
						if (!wordMap.get(w).contains(currentVerseID)) {
							wordMap.put(w, wordMap.get(w) + "," + currentVerseID);
						}	else {
								continue;
						}
					}
				}
				String tmp;
				for (Map.Entry<String, String> entry : wordMap.entrySet()) {
					word.set(entry.getKey());
					tmp = entry.getValue();
					output.set_verseIDList(tmp);
					context.write(word, output);
				}
			}
//			wordMap.clear();
		}
	}


	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, InvertedIndexWritable, Text, InvertedIndexWritable> {

		InvertedIndexWritable output = new InvertedIndexWritable();
		String verseIDList;

		@Override
		public void reduce(Text key, Iterable<InvertedIndexWritable> values, Context context)
				throws IOException, InterruptedException {
//			String verseIDList = "";

			for (InvertedIndexWritable value : values) {
//				word = new Text(value.get_word());
				verseIDList = value.get_verseIDList();
//				output.set_verseIDList(verseIDList);
//				context.write(key, output);
			}
//			verseIDList = value.get_output();
			output.set_verseIDList(verseIDList);
			context.write(key, output);
		}
	}


    /**
     * The run method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Job job = Job.getInstance(conf, "InvertedIndex");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(InvertedIndex.class);

		// Set the output key and value types (for map and reduce).
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InvertedIndexWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InvertedIndexWritable.class);
		// Set the map and reduce classes (and combiner, if used).
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setCombinerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPaths(job, appArgs[0]);
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
			int res = ToolRunner.run(new InvertedIndex(), args);
			System.exit(res);
    }
}
