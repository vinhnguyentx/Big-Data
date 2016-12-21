package com.refactorlabs.cs378.assign4;

//import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.Schema;
import java.net.URL;
import java.net.URLClassLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.lang.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;


/**
 * WordCount example using AVRO defined class for the word count data,
 * to demonstrate how to use AVRO defined objects.
 */
public class WordStatistics extends Configured implements Tool {


    /**
     * The Map class for word count.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word count example.
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {

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
            WordStatisticsData output = new WordStatisticsData();
            output.setDocumentCount(1L);

            // For each word in the input line, update its frequency
            while (tokenizer.hasMoreTokens()) {
                StringBuilder tokenIn = new StringBuilder(tokenizer.nextToken().toLowerCase());

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
                WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
                word.set(entry.getKey());
                tmp = entry.getValue().longValue();
                builder.setDocumentCount(1L);
//                builder.set_word_freq(tmp);
                builder.setTotalCount(tmp );
                builder.setMin(0L);
                builder.setMax(0L);
                builder.setSumOfSquares(tmp * tmp);

                context.getCounter("Input Lines", "Words In").increment(1L);
                context.write(word, new AvroValue(builder.build()));
            }
            wordMap.clear();
        }
    }

    /**
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticsData>,
            Text, AvroValue<WordStatisticsData>> {

        WordStatisticsData output = new WordStatisticsData();


        @Override
        public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
                throws IOException, InterruptedException {

            long doc_count = 0L;
            long word_freq = 0L;
            long total_count = 0L;
            long min = 0L;
            long max = 0L;
            long sum_of_squares = 0L;
            double mean = 0.0;
            double var = 0.0;

            context.getCounter("Words Out", "Words Out").increment(1L);

            // Sum up the counts for the current word, specified in object "key".
            for (AvroValue<WordStatisticsData> value : values) {
                doc_count += value.datum().getDocumentCount();
//                word_freq += value.datum().get_freq();
                total_count += value.datum().getTotalCount();
                min = (value.datum().getDocumentCount() < min ? value.datum().getDocumentCount() : min);
                max = (value.datum().getDocumentCount() > max ? value.datum().getDocumentCount() : max);
                sum_of_squares += value.datum().getSumOfSquares();
            }

            mean = (double)total_count/(double)doc_count;
            var = (((double)sum_of_squares/(double)doc_count) - (mean*mean));

            // Emit the total count for the word.
            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
            builder.setDocumentCount(doc_count);
            builder.setTotalCount(total_count);
            builder.setMin(min);
            builder.setMax(max);
            builder.setSumOfSquares(sum_of_squares);
            builder.setMean(mean);
            builder.setVariance(var);

            context.write(key, new AvroValue<WordStatisticsData>(builder.build()));
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordStatistics <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "WordStatistics");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatistics.class);

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        AvroJob.setOutputValueSchema(job, WordStatisticsData.getClassSchema());

        // Grab the input file and output directory from the command line.
        String[] inputPaths = appArgs[0].split(",");
        for ( String inputPath : inputPaths ) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }
    
    /**
	 * Writes the classpath to standard out, for inspection.
	*/
    public static void printClassPath() {
    	ClassLoader cl = ClassLoader.getSystemClassLoader();
		URL[] urls = ((URLClassLoader) cl).getURLs();
		System.out.println("classpath BEGIN");
		for (URL url : urls) {
			System.out.println(url.getFile());
		}
		System.out.println("classpath END");
		System.out.flush();
	}

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        //Utils.printClassPath();
        int res = ToolRunner.run(new WordStatistics(), args);
        System.exit(res);
    }

}