package com.refactorlabs.cs378.assign6;

/**
 * Created by vinhnguyen on 10/10/16.
 */

import com.refactorlabs.cs378.sessions.*;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.HashSet;

public class ReduceSideJoin extends Configured implements Tool {

    /**
     * The Map class for Avro input. Extends class Mapper, and use Avro session class
     */
    public static class AvroMapClass extends Mapper<AvroKey<CharSequence>, AvroValue<Session>,
            Text, AvroValue<VinImpressionCounts>> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        public static final String MAPPER_COUNTER_GROUP = "Avro Mapper Count:";

        /**
         * Local variable "word" will contain the word identified in the input.
         */
        private Text word = new Text();

        // store when a event is CLICK
        private HashMap<CharSequence, Long> clickMap = new HashMap<CharSequence, Long>();
        // store the vin when its eventtype is CLICK and kind of the subtype

        private HashMap<String, HashMap<CharSequence, Long>> vinClickMap = new HashMap<String, HashMap<CharSequence, Long>>();

        // store the vin of which Contact_form is edited
        private HashSet<String> contactFormEditedSet = new HashSet<>();

        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
                throws IOException, InterruptedException {

            context.getCounter(MAPPER_COUNTER_GROUP, "Avro Input Lines").increment(1L);


            for (Event e : value.datum().getEvents()) {
                String vin = e.getVin().toString();
                EventType eType = e.getEventType();
                EventSubtype eSubType = e.getEventSubtype();
                // check if vin is already in the map , if not, put it into the map
                if (!vinClickMap.containsKey(vin)) {
                    clickMap = new HashMap();
                    vinClickMap.put(vin, clickMap);
                }
                // check if the event is CLICK, if yes, add all event subtypes
                if (eType == EventType.CLICK) {
                    vinClickMap.get(vin).put(eSubType.toString(), 1L);
                // if the event is EDIT and its subtype is Contact_form
                } else if (eType == EventType.EDIT && eSubType == eSubType.CONTACT_FORM) {
                    contactFormEditedSet.add(vin);
                }
            }

            VinImpressionCounts.Builder impressionBuilder;
            for (String v : vinClickMap.keySet()) {
                impressionBuilder = VinImpressionCounts.newBuilder();
                impressionBuilder.setUniqueUsers(1L);
                clickMap = vinClickMap.get(v);

                // check if clickMap is not empty, if yes, set it into the output
                if (clickMap.size() > 0) {
                    impressionBuilder.setClicks(clickMap);
                }

                // check if Contact_form is edited with the vin, if yes, increment by 1
                if (contactFormEditedSet.contains(v)) {
                    impressionBuilder.setEditContactForm(1L);
                }

                word.set(v);
                context.write(word, new AvroValue(impressionBuilder.build()));

                context.getCounter(MAPPER_COUNTER_GROUP, "Avro Mapper output").increment(1L);
                clickMap.clear();
            }
            vinClickMap.clear();
            contactFormEditedSet.clear();
        }
    }


    /**
     * The Map class for CSV input. Extends class Mapper, and use Avro session class
     */
    public static class ImpressionMapClass extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {

        /**
         * Counter group for the mapper.  Individual counters are grouped for the mapper.
         */
        public static final String MAPPER_COUNTER_GROUP = "CSV Mapper Count:";

        /**
         * Local variable "word" will contain the word identified in the input.
         */
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.getCounter(MAPPER_COUNTER_GROUP, "CSV Input Lines").increment(1L);

            VinImpressionCounts.Builder vinBuilder = VinImpressionCounts.newBuilder();

            String[] output = value.toString().split(",");

            if (output[0].equals(null)) {
                return;
            } else if (output[1].equals("VDP")) {
                vinBuilder.setMarketplaceVdps(Long.parseLong(output[2]));
            } else if (output[1].equals("SRP")) {
                vinBuilder.setMarketplaceSrps(Long.parseLong(output[2]));
            }

            word.set(output[0]);
            context.write(word, new AvroValue(vinBuilder.build()));

            context.getCounter(MAPPER_COUNTER_GROUP, "CSV Mapper output").increment(1L);
        }
    }


    /**
     * The Reduce class for user sessions. Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word statistics.
     */
     public static class ReduceClass extends Reducer<Text, AvroValue<VinImpressionCounts>, Text, AvroValue<VinImpressionCounts>> {

         /**
          * Counter group for the reducer.  Individual counters are grouped for the reducer.
          */
         private static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

        /**
         * Local variable "word" will contain the word identified in the input.
         */
         private Text word = new Text();

         public static VinImpressionCounts.Builder vinImpressionBuilder(Iterable<AvroValue<VinImpressionCounts>> values) {

             Long uniqueUsers = 0L;
             HashMap<CharSequence, Long> clicks = new HashMap<>();
             Long edit_contact_form = 0L;
             Long marketplace_srps = 0L;
             Long marketplace_vdps = 0L;

             for (AvroValue<VinImpressionCounts> value : values) {
                 VinImpressionCounts data = value.datum();

                 uniqueUsers += data.getUniqueUsers();
                 edit_contact_form += data.getEditContactForm();
                 marketplace_srps += data.getMarketplaceSrps();
                 marketplace_vdps += data.getMarketplaceVdps();

                 if (data.getClicks() != null) {
                     for (CharSequence key: data.getClicks().keySet()) {
                         if (clicks.containsKey(key)) {
                             clicks.put(key, clicks.get(key) + data.getClicks().get(key));
                         } else {
                             clicks.put(key, data.getClicks().get(key));
                         }
                     }
                 }
             }

             VinImpressionCounts.Builder output = VinImpressionCounts.newBuilder();
                output.setUniqueUsers(uniqueUsers);
                output.setClicks(clicks);
                output.setEditContactForm(edit_contact_form);
                output.setMarketplaceSrps(marketplace_srps);
                output.setMarketplaceVdps(marketplace_vdps);

             return output;
         }

         @Override
         public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
                 throws IOException, InterruptedException {

             context.getCounter(REDUCER_COUNTER_GROUP, "Reducer Input Counts").increment(1L);

             VinImpressionCounts.Builder vinImpressionCountsBuilder = vinImpressionBuilder(values);

             if (vinImpressionCountsBuilder.getUniqueUsers() == 0) {
                 return;
             }
             word.set(key);
             context.write(word, new AvroValue(vinImpressionCountsBuilder.build()));

             context.getCounter(REDUCER_COUNTER_GROUP, "Reducer Output Counts").increment(1L);
         }
     }


    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: ReduceSideJoin <avro input path> <impression input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();

        // Use this JAR first in the classpath (We also set a bootstrap script in AWS)
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "ReduceSideJoin");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(ReduceSideJoin.class);

        // Specify the Map
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());
        AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        AvroJob.setOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Set the input file and output directory from the command line.
        MultipleInputs.addInputPath(job,new Path(appArgs[0]),AvroKeyValueInputFormat.class,AvroMapClass.class);
        MultipleInputs.addInputPath(job,new Path(appArgs[1]),TextInputFormat.class, ImpressionMapClass.class);
        FileOutputFormat.setOutputPath(job, new Path(appArgs[2]));

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
        int res = ToolRunner.run(new ReduceSideJoin(), args);
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
