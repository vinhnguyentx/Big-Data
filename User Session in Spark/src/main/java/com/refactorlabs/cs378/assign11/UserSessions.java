package com.refactorlabs.cs378.assign11;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import org.apache.spark.util.LongAccumulator;
import org.apache.spark.Partitioner;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

import java.io.Serializable;
import java.util.Set;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Comparator;
import java.util.Random;


/**
 * WordCount application for Spark.
 */
public class UserSessions {

	public static class Event implements Comparable<Event>, Serializable {
        String eventType;
        String eventSubType;
        String eventTimestamp;
        public String toString() { return "<" + eventType + ":" + eventSubType + "," + eventTimestamp + ">";}

        @Override
        public int compareTo(Event other) {
			if (!this.eventTimestamp.equals(other.eventTimestamp)) {
				return this.eventTimestamp.compareTo(other.eventTimestamp);
			} else {
				if (this.eventType.equals(other.eventType)) {
					return this.eventSubType.compareTo(other.eventSubType);
				} else {
					return this.eventType.compareTo(other.eventType);
				}
			}
        }
    }

	private static class CustomPartitioner extends Partitioner {

		public int numPartitions() {
			return 6;
		}

        public int getPartition(java.lang.Object key) {
            return Math.abs(((Tuple2) key)._2().hashCode() % 6);
        }
    }

	// compare userID first, then the referring domain
	public static class TupleComparator implements Comparator<Tuple2<Long, String>>, Serializable {
	    @Override
	    public int compare(Tuple2<Long, String> tup1, Tuple2<Long, String> tup2) {
	        if (tup1._1() != tup2._1()) {
				return tup1._1().compareTo(tup2._1());
	        } else {
				return tup1._2().compareTo(tup2._2());
	        }
	    }
	}

	public static void main(String[] args) {
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		Utils.printClassPath();

		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(UserSessions.class.getName()).setMaster("local");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// Load the input data
		JavaRDD<String> input = jsc.textFile(inputFilename);

		// Create a Spark context
		SparkContext sc = jsc.sc();

	 	final LongAccumulator showerSessionCount = sc.longAccumulator();
		final LongAccumulator filteredSessionCount = sc.longAccumulator();
		final LongAccumulator eventCount = sc.longAccumulator();
		final LongAccumulator duplicateEventCount = sc.longAccumulator();
		final LongAccumulator filteredEventCount = sc.longAccumulator();

		// Split events from each line and return pairs of (userID, list of event)
		PairFunction<String, Tuple2<Long, String>, TreeSet<Event>> pairFunction =
				new PairFunction<String, Tuple2<Long, String>, TreeSet<Event>>() {
			@Override
			public Tuple2<Tuple2<Long, String>, TreeSet<Event>> call (String s) throws Exception {
				if (s.length() == 0) {
                    return null;
                }
				// Split the event and increment count
				String[] line = s.split("\t");
				eventCount.add(1);

				// get userID and referring domain
				Tuple2<Long, String> userIdAndDomain = new Tuple2<>(Long.parseLong(line[0]), line[3]);

				// get event info
				Event event = new Event();
				String typeOfEvent = line[1].split(" ")[0];
				event.eventType = typeOfEvent;
				event.eventSubType = line[1].substring(typeOfEvent.length() + 1);
				event.eventTimestamp = line[4];

				// creat list of event
				TreeSet<Event> eventTreeSet = new TreeSet<Event>();

				if (event.eventSubType.toLowerCase().equals("contact form") ||
						event.eventType.toLowerCase().equals("click") ||
							event.eventType.toLowerCase().equals("show") ||
								event.eventType.toLowerCase().equals("display")) {
					eventTreeSet.add(event);
				}
				// eventTreeSet.add(event);

				return new Tuple2<>(userIdAndDomain, eventTreeSet);
			}
		};

		// Combine the events
		Function2<TreeSet<Event>, TreeSet<Event>, TreeSet<Event>> combineFunction =
                new Function2<TreeSet<Event>, TreeSet<Event>, TreeSet<Event>>() {
            @Override
            public TreeSet<Event> call(TreeSet<Event> treeSet1, TreeSet<Event> treeSet2) throws Exception {
                TreeSet<Event> combinedTreeSet = new TreeSet<Event>();
				for (Event event : treeSet1) {
					combinedTreeSet.add(event);
				}
                for (Event event : treeSet2) {
                    // increment accumulator if duplicate events exist
                    if (!combinedTreeSet.add(event)) {
						duplicateEventCount.add(1);
					}
                }
				return combinedTreeSet;
            }
        };

		// filter out SHOWER sessions
		Function<Tuple2<Tuple2<Long, String>, TreeSet<Event>>, Boolean> filterFunction =
				new Function<Tuple2<Tuple2<Long, String>, TreeSet<Event>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple2<Long, String>, TreeSet<Event>> treeSet) throws Exception {
                TreeSet<Event> filteredTreeSet = new TreeSet<Event>();
				int contactFormCount = 0;
                int clickCount = 0;
                int showOrDisplayCount = 0;

				for (Event event: treeSet._2) {
                    if (event.eventSubType.toLowerCase().equals("contact form")) {
                        contactFormCount++;
                    }

                    if (event.eventType.toLowerCase().equals("click")) {
                        clickCount++;
                    }

                    if (event.eventType.toLowerCase().equals("show") || event.eventType.toLowerCase().equals("display")) {
                        showOrDisplayCount++;
                    }
                }

				if (contactFormCount == 0 && clickCount == 0 && showOrDisplayCount > 0) {

					showerSessionCount.add(1);
					Random random = new Random();
					double randomNum = random.nextDouble();

					if (randomNum > 0.10) {
                        // increment accumulator
                        filteredSessionCount.add(1);
                        filteredEventCount.add(treeSet._2.size());

						return false;
                    } else {
						return true;
					}
				} else {
					return true;
				}
            }
        };

		JavaPairRDD<Tuple2<Long, String>, TreeSet<Event>> userSessions = input.mapToPair(pairFunction);
        JavaPairRDD<Tuple2<Long, String>, TreeSet<Event>> combinedUserSessions =
				userSessions.reduceByKey(combineFunction);
        JavaPairRDD<Tuple2<Long, String>, TreeSet<Event>> partitionedUserSessions =
				combinedUserSessions.partitionBy(new CustomPartitioner());
        JavaPairRDD<Tuple2<Long, String>, TreeSet<Event>> sortedUserSessions =
				partitionedUserSessions.sortByKey(new TupleComparator());
		sortedUserSessions.filter(filterFunction)
        		.saveAsTextFile(outputFilename);

		// print out statistics
		System.out.println("Total number of events after duplicates removed: " + (eventCount.value() - duplicateEventCount.value()));
        System.out.println("Total number of events after session filtering: " + (eventCount.value() - duplicateEventCount.value() - filteredEventCount.value()));
        System.out.println("Total number of sessions: " + sortedUserSessions.count());
        System.out.println("Total number of sessions of type SHOWER: " + showerSessionCount.value());
        System.out.println("Total number of sessions of type SHOWER that were filtered out: " + filteredSessionCount.value());

		// Shut down the context
		sc.stop();
	}

}
