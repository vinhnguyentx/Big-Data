package com.refactorlabs.cs378.assign3;

//Import
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.io.Text;

public class InvertedIndexWritable implements Writable {
//	String word;
//	Text word;
	String verseIDList;
	
	public InvertedIndexWritable() {
		this.initialize();
	}
	
	public InvertedIndexWritable(String verseIDList) {
//		this.word = word;
		this.verseIDList = verseIDList;
	}
	
	public void initialize() {
		verseIDList = "";
	}
	
	public void readFields(DataInput in) throws IOException { 
		// Read the data out in the order it is written
//		String s = in.readLine();
//		verseIDList = new ArrayList<String>(Arrays.asList(s.split(",")));
//		word = new Text(in.readLine());
		verseIDList = in.readLine();
	}
	
	public void write(DataOutput out) throws IOException {
		// Write the data out in the order it is read
//		out.writeChars(arrayToString(verseIDList));
//		out.writeChars(word.toString());
		out.writeChars(verseIDList);
	}
	
	public String toString() {
		return String.format("%s", verseIDList);
	}
	
//	public void set_word(Text w) {
//		word = w;
//	}
//	
//	public String get_word() {
//		return word.toString();
//	}
	
	public void set_verseIDList(String v) {
		verseIDList = v;
	}
	
	public String get_verseIDList() {
		return verseIDList;
	}
	
////	@Override
//	public static String arrayToString(ArrayList<String> al) {
////		StringBuilder sb = new Stringbuilder();
////		for (String s : al) {
////			sb.append(s);
////			sb.append(" ");
////		}
////		System.out.println(sb.toString());
//		String s = String.join(",", al);
//		return s;
//	}
}