package com.refactorlabs.cs378.assign8;

import com.refactorlabs.cs378.sessions.Event;
import com.refactorlabs.cs378.sessions.EventType;
import com.refactorlabs.cs378.sessions.EventSubtype;
import com.refactorlabs.cs378.sessions.Session;
import com.refactorlabs.cs378.sessions.SessionType;
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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.HashMap;
import java.net.URL;
import java.net.URLClassLoader;

public class JobChaining extends Configured implements Tool {

    /**
     * The Map class for Sessions.  Extends class Mapper, provided by Hadoop.
     */
    public static class MapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>,
            AvroKey<CharSequence>, AvroValue<Session>> {

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

//        private Random rand = new Random();

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
     * The Map class for Job chaining.  Extends class Mapper, provided by Hadoop.
     */
    public static class ChainingMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>,
            AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        private static final String MAPPER_COUNTER_GROUP = "Session Counts";

        HashMap<EventSubtype, Long> subtypeMap = new HashMap<>();

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {

            context.getCounter(MAPPER_COUNTER_GROUP, getSessionType().getText()).increment(1L);

            // Count eventSubtypes of each Session type
            for (Event event : value.datum().getEvents()) {
                EventSubtype eventSubtype = event.getEventSubtype();
                if (!subtypeMap.containsKey(eventSubtype)) {
                    subtypeMap.put(eventSubtype, 1L);
                } else {
                    subtypeMap.put(eventSubtype, subtypeMap.get(eventSubtype) + 1L);
                }
            }

            EventSubtypeStatisticsKey.Builder keyBuilder = EventSubtypeStatisticsKey.newBuilder();

            EventSubtypeStatisticsData.Builder outputBuilder = EventSubtypeStatisticsData.newBuilder();
            for (EventSubtype eventSubtype : EventSubtype.values()) {
                Long count = 0L;
                if (subtypeMap.containsKey(eventSubtype)) {
                    count = subtypeMap.get(eventSubtype);
                }

                // set values for key
                keyBuilder.setSessionType(this.getSessionType().toString());
                keyBuilder.setEventSubtype(eventSubtype.toString());

                // set values for output
                outputBuilder.setSessionCount(1L);
                outputBuilder.setTotalCount(count);
                outputBuilder.setSumOfSquares(count * count);
                outputBuilder.setMean(0);
                outputBuilder.setVariance(0);

                context.write(new AvroKey(keyBuilder.build()), new AvroValue(outputBuilder.build()));
            }
            subtypeMap.clear();
        }

        public SessionType getSessionType() {
            return SessionType.OTHER;
        }

    }

    // Mapper class for submitter session
    public static class SubmitterMapper extends ChainingMapper{

        @Override
        public SessionType getSessionType() {
            return SessionType.SUBMITTER;
        }
    }

    // Mapper class for clicker session
    public static class ClickerMapper extends ChainingMapper{

        @Override
        public SessionType getSessionType() {
            return SessionType.CLICKER;
        }
    }

    // Mapper class for shower session
    public static class ShowerMapper extends ChainingMapper{

        @Override
        public SessionType getSessionType() {
            return SessionType.SHOWER;
        }
    }

    // Mapper class for visitor session
    public static class VisitorMapper extends ChainingMapper{

        @Override
        public SessionType getSessionType() {
            return SessionType.VISITOR;
        }
    }


    /**
     * Aggregate Mapper class for all session types
     */
    public static class AggregateMapper extends Mapper<AvroKey<EventSubtypeStatisticsKey>,
            AvroValue<EventSubtypeStatisticsData>, AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {

        @Override
        public void map(AvroKey<EventSubtypeStatisticsKey> key, AvroValue<EventSubtypeStatisticsData> value, Context context)
                throws IOException, InterruptedException {

            // statistics for each event subtype for each session type
            context.write(key, value);

            EventSubtypeStatisticsKey.Builder outputBuilder = EventSubtypeStatisticsKey.newBuilder();

            // statistics for each session type
            outputBuilder.setSessionType(key.datum().getSessionType());
            outputBuilder.setEventSubtype("ANY");
            context.write(new AvroKey(outputBuilder.build()), value);

            // statistics for each event subtype of all session type
            outputBuilder.setSessionType("ANY");
            outputBuilder.setEventSubtype(key.datum().getEventSubtype());
            context.write(new AvroKey(outputBuilder.build()), value);

            // statistics for all session types together
            outputBuilder.setEventSubtype("ANY");
            context.write(new AvroKey(outputBuilder.build()), value);
        }
    }

    /**
     * Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word statistics.
     */
    public static class ReduceClass extends Reducer<AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>,
            AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {

        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

        @Override
        public void reduce(AvroKey<EventSubtypeStatisticsKey> key, Iterable<AvroValue<EventSubtypeStatisticsData>> values, Context context)
                throws IOException, InterruptedException {

            long sumOfSession = 0L;
            long sumOfCount = 0L;
            long sumOfSquares = 0L;

            for (AvroValue<EventSubtypeStatisticsData> value : values) {
                sumOfSession = sumOfSession + value.datum().getSessionCount();
                sumOfCount = sumOfCount + value.datum().getTotalCount();
                sumOfSquares = sumOfSquares + value.datum().getSumOfSquares();
            }

            double mean = (double)sumOfCount / (double)sumOfSession;

            EventSubtypeStatisticsData.Builder outputBuilder = EventSubtypeStatisticsData.newBuilder();
            outputBuilder.setSessionCount(sumOfSession);
            outputBuilder.setTotalCount(sumOfCount);
            outputBuilder.setSumOfSquares(sumOfSquares);
            outputBuilder.setMean(mean);
            outputBuilder.setVariance(((double) sumOfSquares / sumOfSession) - (mean * mean));

            AvroValue outputValue = new AvroValue(outputBuilder.build());
            context.write(key, outputValue);
        }
    }

    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {

        if (args.length != 11) {
            System.err.println("Usage: JobChaining <input path 0> <output path 0> <input path 1> <output path 1> <input path 2> " +
                    "<output path 2> <input path 3> <output path 3> <input path 4> <output path 4> <output path 2>");
            return -1;
        }

        Configuration conf = getConf();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        /**
         * Job 0: Session Binding
         */
        Job job0 = Job.getInstance(conf, "SessionBinning");

        // Identify the JAR file to replicate to all machines.
        job0.setJarByClass(JobChaining.class);

        // Set the Map
        job0.setInputFormatClass(AvroKeyValueInputFormat.class);
        job0.setMapperClass(MapClass.class);
        AvroJob.setInputKeySchema(job0, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job0, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(job0, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job0, Session.getClassSchema());
        AvroJob.setOutputKeySchema(job0, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job0, Session.getClassSchema());
        job0.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job0.setNumReduceTasks(0);

        // Set the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job0, appArgs[0]);
        FileOutputFormat.setOutputPath(job0, new Path(appArgs[1]));

        // Set multiple outputs
        for (SessionType sessionType : SessionType.values()) {
            AvroMultipleOutputs.addNamedOutput(job0, sessionType.getText(), AvroKeyValueOutputFormat.class,
                    Schema.create(Schema.Type.STRING), Session.getClassSchema());
        }
        AvroMultipleOutputs.setCountersEnabled(job0, true);

        // Wait for completion.
        job0.waitForCompletion(true);


        /**
         * Job 1: Submitter Mapper
         */
        Job job1 = Job.getInstance(conf, "SubmitterMapper");
        job1.setJarByClass(JobChaining.class);

        // Set the Map
        job1.setMapperClass(SubmitterMapper.class);

        // Set the Reduce
        job1.setReducerClass(ReduceClass.class);

        // Set the Map
        job1.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job1, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job1, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(job1, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(job1, EventSubtypeStatisticsData.getClassSchema());
        job1.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        AvroJob.setOutputKeySchema(job1, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(job1, EventSubtypeStatisticsData.getClassSchema());

        FileInputFormat.addInputPaths(job1, appArgs[2]);
        FileOutputFormat.setOutputPath(job1, new Path(appArgs[3]));


        /**
         * Job 2: Clicker Mapper
         */
        Job job2 = Job.getInstance(conf, "ClickerMapper");
        job2.setJarByClass(JobChaining.class);

        // Set the Map
        job2.setMapperClass(ClickerMapper.class);

        // Set the Reduce
        job2.setReducerClass(ReduceClass.class);

        // Set the Map
        job2.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job2, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job2, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(job2, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(job2, EventSubtypeStatisticsData.getClassSchema());
        job2.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        AvroJob.setOutputKeySchema(job2, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(job2, EventSubtypeStatisticsData.getClassSchema());

        FileInputFormat.addInputPaths(job2, appArgs[4]);
        FileOutputFormat.setOutputPath(job2, new Path(appArgs[5]));


        /**
         * Job 3: Shower Mapper
         */
        Job job3 = Job.getInstance(conf, "ShowerMapper");
        job3.setJarByClass(JobChaining.class);

        // Set the Map
        job3.setMapperClass(ShowerMapper.class);

        // Set the Reduce
        job3.setReducerClass(ReduceClass.class);

        // Set the Map
        job3.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job3, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job3, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(job3, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(job3, EventSubtypeStatisticsData.getClassSchema());
        job3.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        AvroJob.setOutputKeySchema(job3, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(job3, EventSubtypeStatisticsData.getClassSchema());

        FileInputFormat.addInputPaths(job3, appArgs[6]);
        FileOutputFormat.setOutputPath(job3, new Path(appArgs[7]));


        /**
         * Job 4: Visitor Mapper
         */
        Job job4 = Job.getInstance(conf, "VisitorMapper");
        job4.setJarByClass(JobChaining.class);

        // Set the Map
        job4.setMapperClass(VisitorMapper.class);

        // Set the Reduce
        job4.setReducerClass(ReduceClass.class);

        // Set the Map
        job4.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job4, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job4, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(job4, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(job4, EventSubtypeStatisticsData.getClassSchema());
        job4.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        AvroJob.setOutputKeySchema(job4, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(job4, EventSubtypeStatisticsData.getClassSchema());

        FileInputFormat.addInputPaths(job4, appArgs[8]);
        FileOutputFormat.setOutputPath(job4, new Path(appArgs[9]));


        job1.submit();
        job2.submit();
        job3.submit();
        job4.submit();

        while (!(job1.isComplete() && job2.isComplete() && job3.isComplete() && job4.isComplete())) {
            Thread.sleep(2000);
        }

        /**
         * Job 5: Aggregate Mapper
         */
        Job job5 = Job.getInstance(conf, "AggregateMapper");

        job5.setJarByClass(JobChaining.class);

        // Specify the Map
        job5.setMapperClass(AggregateMapper.class);

        // Specify the Reduce
        job5.setReducerClass(ReduceClass.class);

        // Set the Map
        job5.setInputFormatClass(AvroKeyValueInputFormat.class);
        AvroJob.setInputKeySchema(job5, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setInputValueSchema(job5, EventSubtypeStatisticsData.getClassSchema());
        AvroJob.setMapOutputKeySchema(job5, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(job5, EventSubtypeStatisticsData.getClassSchema());
        job5.setOutputFormatClass(TextOutputFormat.class);
        AvroJob.setOutputKeySchema(job5, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(job5, EventSubtypeStatisticsData.getClassSchema());

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPath(job5, new Path(appArgs[3]));
        FileInputFormat.addInputPath(job5, new Path(appArgs[5]));
        FileInputFormat.addInputPath(job5, new Path(appArgs[7]));
        FileInputFormat.addInputPath(job5, new Path(appArgs[9]));
        FileOutputFormat.setOutputPath(job5, new Path(appArgs[10]));

        job5.waitForCompletion(true);

        return 0;
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        printClassPath();
        int res = ToolRunner.run(new JobChaining(), args);
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