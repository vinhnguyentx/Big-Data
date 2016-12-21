package com.refactorlabs.cs378.assign7;

/**
 * Created by vinhnguyen on 10/10/16.
 */

import com.refactorlabs.cs378.sessions.*;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Random;
import java.net.URL;
import java.net.URLClassLoader;

public class SessionBinning extends Configured implements Tool {

    /**
     * The Map class for Sessions.  Extends class Mapper, provided by Hadoop.
     */
    public static class MapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Mapper Counts";

        /**
         * Counter group for the sessions.  Individual counters are grouped for the sessions.
         */
        private static final String SESSION_COUNTER_GROUP = "Session Counts";

        /**
         * Local variable "word" will contain the word identified in the input.
         */

        /**
         * Local variable "word" will contain the word identified in the input.
         * The Hadoop Text object is mutable, so we can reuse the same object and
         * simply reset its value as each word in the input is encountered.
         */
        private Text word = new Text();

        private AvroMultipleOutputs outputs;

        private Random rand = new Random();

        @Override
        protected void setup(Context context) {
            outputs = new AvroMultipleOutputs(context);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {

            Session session = value.datum();

            // Check if the sessions has more than 100 events
            if (session.getEvents().size() > 100) {
                context.getCounter(MAPPER_COUNTER_GROUP, "Large Sessions Discarded").increment(1L);
                return;
            }

            boolean submitter = false;
            boolean clicker = false;
            boolean shower = false;
            boolean visitor = false;

            for (Event event: value.datum().getEvents()) {
                EventType eventType = event.getEventType();

                if (eventType == EventType.CHANGE || (eventType == EventType.EDIT && event.getEventSubtype() == EventSubtype.CONTACT_FORM)) {
                    submitter = true;
                } else if (eventType == EventType.CLICK) {
                    clicker = true;
                } else if (eventType == EventType.SHOW || eventType == EventType.DISPLAY) {
                    shower = true;
                } else if (eventType == EventType.VISIT) {
                    visitor = true;
                }

            }

            if (submitter) {
                outputs.write("submitter", key,	value);
                context.getCounter(SESSION_COUNTER_GROUP, "submitter").increment(1L);
            } else if (clicker) {
//                if (rand.nextDouble() > 0.10) {
//                    context.getCounter(MAPPER_COUNTER_GROUP, "Clicker Sessions Discarded").increment(1L);
//                    return;
//                } else {
                    outputs.write("clicker", key, value);
                    context.getCounter(SESSION_COUNTER_GROUP, "clicker").increment(1L);
//                }
            } else if (shower) {
//                if (rand.nextDouble() > 0.02) {
//                    context.getCounter(MAPPER_COUNTER_GROUP, "Shower Sessions Discarded").increment(1L);
//                    return;
//                } else {
                    outputs.write("shower", key, value);
                    context.getCounter(SESSION_COUNTER_GROUP, "shower").increment(1L);
//                }
            } else if (visitor) {
                outputs.write("visitor", key, value);
                context.getCounter(SESSION_COUNTER_GROUP, "visitor").increment(1L);
            } else {
                outputs.write("other", key,	value);
                context.getCounter(SESSION_COUNTER_GROUP, "other").increment(1L);
            }
        }
    }


    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: SessionBinning <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "SessionBinning");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(SessionBinning.class);

        // Specify the Map
        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        job.setMapperClass(MapClass.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setNumReduceTasks(0);

        // Set the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Set multiple outputs
        for (SessionType sessionType : SessionType.values()) {
            AvroMultipleOutputs.addNamedOutput(job, sessionType.getText(), AvroKeyValueOutputFormat.class,
                    Schema.create(Schema.Type.STRING), Session.getClassSchema());
        }
        AvroMultipleOutputs.setCountersEnabled(job, true);

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);
        return 0;
    }


    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        printClassPath();
        int res = ToolRunner.run(new SessionBinning(), args);
        System.exit(res);
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
}