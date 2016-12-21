package com.refactorlabs.cs378.assign5;

/**
 * Created by vinhnguyen on 10/04/16.
 */

import com.refactorlabs.cs378.sessions.*;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;

public class UserSessions extends Configured implements Tool {

    /**
     * The Map class for word count. Extends class Mapper, and use Avro session class
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        public static final String MAPPER_COUNTER_GROUP = "Mapper Count:";

        /**
         * Local variable "word" will contain the word identified in the input.
         */
        private Text word = new Text();

        private final static String[] fieldNames = {"user_id", "event_type", "page", "referring_domain",
                "event_timestamp", "city", "vin", "vehicle_condition", "year", "make", "model", "trim",
                "body_style", "cab_style", "price", "mileage", "image_count", "free_carfax_report", "features"};

        private Event event = new Event();
        private String[] eventType;
        private String token;
        private ArrayList<CharSequence> features = new ArrayList<>();
        private ArrayList<Event> eventList = new ArrayList<>();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.split("\t");

            context.getCounter(MAPPER_COUNTER_GROUP, "Input Lines").increment(1L);

            // Map event of the userID
            // with (key, value) = (userID, session)
            word.set(tokens[0]);
            Session.Builder builder = Session.newBuilder();

            // Set event type
            eventType = tokens[1].split(" ");

            this.token = eventType[0].toUpperCase();
            event.setEventType(EventType.valueOf(this.token));

            // Set subtype
            this.token = eventType[1].toUpperCase();
            if (eventType.length > 2) {
                this.token = this.token + "_" + eventType[2].toUpperCase();
            }
            event.setEventSubtype(EventSubtype.valueOf(this.token));

            // Set page
            if (tokens[2].equals("null")|| tokens[2].equals("")) {
                event.setPage(null);
            } else {
                event.setPage(tokens[2]);
            }

            // Set referring domain
            if (tokens[3].equals("null")|| tokens[3].equals("")) {
                event.setReferringDomain(null);
            } else {
                event.setReferringDomain(tokens[3]);
            }

            // Set event time stamp
            if (tokens[4].equals("null")|| tokens[4].equals("")) {
                event.setEventTime(null);
            } else {
                event.setEventTime(tokens[4]);
            }

            // Set city
            if (tokens[5].equals("null")|| tokens[5].equals("")) {
                event.setCity(null);
            } else {
                event.setCity(tokens[5]);
            }

            // Set vin
            if (tokens[6].equals("null")|| tokens[6].equals("")) {
                event.setVin(null);
            } else {
                event.setVin(tokens[6]);
            }

            // Set vehicle condition
            this.token = tokens[7];
            event.setCondition(Condition.valueOf(this.token));

            // Set year
            long year = Long.parseLong(tokens[8]);
            event.setYear(year);

            // Set make
            if (tokens[9].equals("null")|| tokens[9].equals("")) {
                event.setMake(null);
            } else {
                event.setMake(tokens[9]);
            }

            // Set model
            if (tokens[10].equals("null")|| tokens[10].equals("")) {
                event.setModel(null);
            } else {
                event.setModel(tokens[10]);
            }

            // Set trim
            if (tokens[11].equals("null")|| tokens[11].equals("")) {
                event.setTrim(null);
            } else {
                event.setTrim(tokens[11]);
            }

            // Set vehicle body style
            this.token = tokens[12];
            event.setBodyStyle(BodyStyle.valueOf(this.token));

            // Set cab style
            this.token = tokens[13];
            if (this.token.equals("null")|| tokens[13].equals("")) {
                event.setCabStyle(null);
            } else {
                event.setCabStyle(CabStyle.valueOf(this.token.split(" ")[0]));
            }

            // Set price
            this.token = tokens[14];
            event.setPrice(Double.parseDouble(this.token));

            // Set mileage
            this.token = tokens[15];
            event.setMileage(Long.parseLong(this.token));

            // Set image count
            this.token = tokens[16];
            event.setImageCount(Long.parseLong(this.token));

            // Set free carfax report
            this.token = tokens[17];
            if (this.token.equals("t")) {
                event.setFreeCarfaxReport(true);
            } else {
                event.setFreeCarfaxReport(false);
            }

            // Set vehicle features
            if (tokens[18].equals("null")|| tokens[18].equals("")) {
                event.setFeatures(null);
            } else {
                features.clear();
                eventType = tokens[18].split(":");
                for (int i = 0; i < eventType.length; i++) {
                    features.add(eventType[i]);
                }

                // Sort list of events
                Collections.sort(features, new Comparator<CharSequence>() {
                    @Override
                    public int compare(CharSequence charSeq1, CharSequence charSeq2) {
                        return charSeq1.toString().compareTo(charSeq2.toString());
                    }
                });
                event.setFeatures(features);
            }

            builder.setUserId(tokens[0]);
            eventList.clear();
            eventList.add(event);
            builder.setEvents(eventList);
            context.write(word, new AvroValue<Session>(builder.build()));
            context.getCounter(MAPPER_COUNTER_GROUP, "Sessions Output").increment(1L);
        }
    }


    /**
     * The Reduce class for user sessions. Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word statistics.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

        /**
         * Counter group for the reducer.  Individual counters are grouped for the reducer.
         */
        private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

        private ArrayList<Event> eventList = new ArrayList<>();
        private Event event;
        private Set<String> eventSet = new HashSet<>();
        private Event.Builder eventBuilder;

        @Override
        public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
                throws IOException, InterruptedException {

            context.getCounter(REDUCER_COUNTER_GROUP, "Session in").increment(1L);

            Session.Builder builder = Session.newBuilder();
            builder.setUserId(key.toString());
            eventList.clear();
            eventSet.clear();

            for (AvroValue<Session> session: values){
                event = session.datum().getEvents().get(0);

                // check for duplicates
                if (eventSet.add(event.toString())) {
                    eventBuilder = Event.newBuilder(event);
                    eventList.add(eventBuilder.build());
                }
            }

            // Sort list of events
            Collections.sort(eventList, new Comparator<Event>() {
                @Override
                public int compare(Event charSeq1, Event charSeq2) {
                    String sq1 = charSeq1.getEventTime().toString();
                    String sq2 = charSeq2.getEventTime().toString();
                    if (sq1.equals(sq2)) {
                        return charSeq1.getEventType().toString().compareTo(charSeq2.getEventType().toString());
                    } else {
                        return sq1.compareTo(sq2);
                    }
                }
            });

            // Set events to the event list
            builder.setEvents(eventList);
            context.write(new AvroKey(key.toString()), new AvroValue<Session>(builder.build()));
        }
    }


    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: UserSessions <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "UserSessions");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(UserSessions.class);

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(MapClass.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

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
        int res = ToolRunner.run(new UserSessions(), args);
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
