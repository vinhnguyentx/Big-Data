/**
 * need to display the right result. right now it's 0, infinity, NaN, and add "'"
 * 
 */

package com.refactorlabs.cs378.assign2;

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
import java.util.HashMap;
//import org.apache.commons.lang3.StringUtils;
import java.lang.*;
//import org.apache.commons.lang.StringUtils;

/**
 * Map Reduce program process Word Statistics 
 * Based on the Example MapReduce program that performs word count.
 * of @author David Franke (dfranke@cs.utexas.edu)
 * 
 * Vinh T Nguyen (vinhnguyentx@utexas.edu)
 */
public class WordStatistics {
	
	/**
	 * The Map class for word count.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function for the word count example.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {

		/**
		 * Local variable "word" will contain the word identified in the input.
		 * The Hadoop Text object is mutable, so we can reuse the same object and
		 * simply reset its value as each word in the input is encountered.
		 */
		private Text word = new Text();
		
		/**
		 * HashMap to store each word and its frequency
		 */
		private static final Map<String, Long> wordMap = new HashMap<String, Long>();

		/**
		 * Punctuation string used to handle words contain "--" or "'" 
		 */
		private String[] punctuations = {"--", ",", ".", ";", ":", "?", "\\", "\'", "=", "_", "!", "(", ")", "\"", "[" , "]"};
		private String[] 	 no_punct = { " ",  "",  "",  "",  "",  "",   "",   "",  "",  "",  "",  "",  "",   "", " [", "] "};


		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// remove special characters
			String line = org.apache.commons.lang.StringUtils.replaceEach(value.toString(), punctuations, no_punct);
			
			if (line.contains("--")) {
				line = line.replace("--", " ");
			}
			// Tokenize the line, delim the special characters in the string
			StringTokenizer tokenizer = new StringTokenizer(line, ",.;:?\"'=_!() -");		
			WordStatisticsWritable output = new WordStatisticsWritable();
			output.set_doc_count(1L);

			// For each word in the input line, update its frequency
			while (tokenizer.hasMoreTokens()) {
				StringBuilder tokenIn = new StringBuilder(tokenizer.nextToken().toLowerCase());

				if (tokenIn.charAt(0) == '[' && tokenIn.charAt(tokenIn.length() - 1) != ']') {
					tokenIn = tokenIn.append("]");
				} else if (tokenIn.charAt(0) != '[' && tokenIn.charAt(tokenIn.length() - 1) == ']') {
					tokenIn = tokenIn.insert(0, "[");
				}

				String token = tokenIn.toString();
				// check if word is on wordMap
				if (wordMap.get(token) == null) {
					// put word in wordMap
					wordMap.put(token, new Long(1));
				} else {
					// increment by 1 if already exists
					wordMap.put(token, new Long(wordMap.get(token) + 1L));
				}
				
			}
			long tmp;
			// iterate through the map
			for (Map.Entry<String, Long> entry : wordMap.entrySet()) {
				word.set(entry.getKey());
				tmp = entry.getValue().longValue();
				output.set_freq(tmp);
				output.set_sumOfSquares(tmp * tmp);
				
				// Emit the final output for the word.
				context.write(word, output);
			}
			wordMap.clear();
		}
	}

	/**
	 * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function for the word count example.
	 */
	public static class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {

		WordStatisticsWritable output = new WordStatisticsWritable();
		
		@Override
		public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
				throws IOException, InterruptedException {
			long doc_count = 0L;
			long word_freq = 0L;
			long sumOfSquares = 0L;
			double mean = 0.0;
			double var = 0.0;
						
			// Sum up the counts for the current word, specified in object "key".
			for (WordStatisticsWritable value : values) {
				doc_count += value.get_doc_count();
				word_freq += value.get_freq();
				sumOfSquares += value.get_sumOfSquares();
			}
			
			mean = (double)word_freq/(double)doc_count;
			var = (((double)sumOfSquares/(double)doc_count) - (mean*mean));
			
			output.set_doc_count(doc_count);
			output.set_freq(word_freq);
			output.set_sumOfSquares(sumOfSquares);
			output.set_mean(mean);
			output.set_var(var);
			
			// Emit the final output for the word.
			context.write(key, output);
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

		Job job = Job.getInstance(conf, "WordStatistics");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(WordStatistics.class);

		// Set the output key and value types (for map and reduce).
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(WordStatisticsWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(WordStatisticsWritable.class);

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