package com.tarento.category.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Custom Writable for Similarity Score within a Category.
 * 
 * @author aravinth
 * @since Jul 31, 2019
 */
public class SimilarityScoreWritable implements Writable {

	public static final String DELIM = "\t";

	private String category;

	private double similarityScore;

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public double getSimilarityScore() {
		return similarityScore;
	}

	public void setSimilarityScore(double similarityScore) {
		this.similarityScore = similarityScore;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.category = in.readUTF();
		this.similarityScore = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(category);
		out.writeDouble(similarityScore);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.setLength(0);

		sb.append(category);
		sb.append(DELIM);

		sb.append(similarityScore);

		return sb.toString();
	}

	public Text toText() {
		return (new Text(toString()));
	}

}