package com.tarento.category.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Custom Writable for Sentence Categorizer.
 * 
 * @author aravinth
 * @since Jul 31, 2019
 */
public class CategoryOutWritable implements Writable {

	public static final String DELIM = "\t";

	private boolean isActualInput;

	private String sentence;

	public boolean isActualInput() {
		return isActualInput;
	}

	public void setActualInput(boolean isActualInput) {
		this.isActualInput = isActualInput;
	}

	public String getSentence() {
		return sentence;
	}

	public void setSentence(String sentence) {
		this.sentence = sentence;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.isActualInput = in.readBoolean();
		this.sentence = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeBoolean(isActualInput);
		out.writeUTF(sentence);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.setLength(0);

		sb.append(isActualInput);
		sb.append(DELIM);

		sb.append(sentence);

		return sb.toString();
	}

	public Text toText() {
		return (new Text(toString()));
	}

}
