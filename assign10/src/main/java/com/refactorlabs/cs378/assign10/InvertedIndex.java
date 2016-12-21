package com.refactorlabs.cs378.assign10;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Comparator;

/**
 * WordCount application for Spark.
 */
public class InvertedIndex {
	public static void main(String[] args) {
		Utils.printClassPath();

		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(InvertedIndex.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load the input data
		JavaRDD<String> input = sc.textFile(inputFilename);

		// Split the input into words
		PairFlatMapFunction<String, String, ArrayList<String>> splitFunction =
				new PairFlatMapFunction<String, String, ArrayList<String>>() {
			@Override
			public Iterator<Tuple2<String, ArrayList<String>>> call(String line) throws Exception {

				List<Tuple2<String, ArrayList<String>>> wordList = Lists.newArrayList();

				if (line.trim().length() == 0) {
                    return wordList.iterator();
                }
                String[] split = line.split(" ", 2);
                String verseID = split[0];

                ArrayList<String> verseIDTreeSet = new ArrayList<>();
				verseIDTreeSet.add(verseID);

				StringTokenizer tokenizer = new StringTokenizer(split[1]);
				Set<String> words = new TreeSet<>();

				// For each word in the input line, emit that word.
				while (tokenizer.hasMoreTokens()) {
                    String nextWord = tokenizer.nextToken().toLowerCase().replaceAll("[^\\p{L}\\p{Nd}]+", "");

					// if (words.add(nextWord)) {
                    //     wordList.add(new Tuple2<String, ArrayList<String>>(nextWord, verseIDTreeSet));
					// }
					if (wordList.contains(nextWord)) {
						verseIDTreeSet.add(verseID);
					} else {
						words.add(nextWord);
						wordList.add(new Tuple2<String, ArrayList<String>>(nextWord, verseIDTreeSet));
					}
				}

				return wordList.iterator();
			}
		};

		// Combine the verseIDs
		Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>> combineFunction =
				new Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>>() {
			@Override
			public ArrayList<String> call(ArrayList<String> treeSet1, ArrayList<String> treeSet2) throws Exception {
				ArrayList<String> combinedTreeSet = new ArrayList<>();
				for (String verse : treeSet1) {
					combinedTreeSet.add(verse);
				}
				for (String verse : treeSet2) {
					combinedTreeSet.add(verse);
				}

				Collections.sort(combinedTreeSet, String.CASE_INSENSITIVE_ORDER);
				// Collections.sort(combinedTreeSet, new Comparator<String>() {
			    //     @Override
			    //     public int compare(String s1, String s2) {
			    //         return s1.compareToIgnoreCase(s2);
			    //     }
			    // });
				return combinedTreeSet;
			}

		};

		JavaPairRDD<String, ArrayList<String>> wordsWithVerse = input.flatMapToPair(splitFunction);
		JavaPairRDD<String, ArrayList<String>> wordsWithAllVerses = wordsWithVerse.reduceByKey(combineFunction);
        JavaPairRDD<String, ArrayList<String>> ordered = wordsWithAllVerses.sortByKey();

		// Save the word count to a text file (initiates evaluation)
		ordered.saveAsTextFile(outputFilename);

		// Shut down the context
		sc.stop();
	}

}
