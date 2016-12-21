package com.refactorlabs.cs378.assign2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class WordStatisticsWritable implements Writable {
	
	long doc_count;
	private long word_freq;
	private long sumOfSquares;
	private double mean;
	private double var;
	
	public WordStatisticsWritable() {
		this.initialize();
	}
		
	public void initialize() {
		doc_count = 0L;
		word_freq = 0L;
		sumOfSquares = 0L;
		mean = 0.0;
		var = 0.0;
	}
	
	public void readFields(DataInput in) throws IOException { 
		// Read the data out in the order it is written
		doc_count = in.readLong();
		word_freq = in.readLong();
	   	sumOfSquares = in.readLong();
		mean = in.readDouble();
		var = in.readDouble();

	}
	
	public void write(DataOutput out) throws IOException {
		// Write the data out in the order it is read
		out.writeLong(doc_count);
		out.writeLong(word_freq);
		out.writeLong(sumOfSquares);
		out.writeDouble(mean);
		out.writeDouble(var);

	}
	
	public String toString() {
		return String.format("%d\t %f %f", doc_count, mean, var);
	}
	
	public void set_doc_count(long x) {
		doc_count = x;
	}
	
	public long get_doc_count() {
		return doc_count;
	}
	
	public void set_freq(long f) {
		word_freq = f;
	}
	
	public long get_freq() {
		return word_freq;
	}
	
	public void set_sumOfSquares(long sum) {
		sumOfSquares = sum;
	}
	
	public long get_sumOfSquares() {
		return sumOfSquares;
	}
	
	public void set_mean(double m) {
		mean = m;
	}
	
	public double get_mean() {
		return mean;
	}
	
	public void set_var(double v) {
		var = v;
	}
	
	public double get_var() {
		return var;
	}
	
	
}