package utils;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableFilterLatinMap implements Writable {

    private WritableWarcRecord record;
    private IntWritable isLatinAlphabet;

    public WritableFilterLatinMap() {
        record = new WritableWarcRecord(new WarcRecord());
        isLatinAlphabet = new IntWritable(1);
    }

    public WritableFilterLatinMap(WritableWarcRecord record, IntWritable isLatinAlphabet) {
        this.record = record;
        this.isLatinAlphabet = isLatinAlphabet;
    }


    public WritableWarcRecord getRecord() {
        return record;
    }

    public void setRecord(WritableWarcRecord record) {
        this.record = record;
    }

    public IntWritable getIsLatinAlphabet() {
        return isLatinAlphabet;
    }

    public void setIsLatinAlphabet(IntWritable isLatinAlphabet) {
        this.isLatinAlphabet = isLatinAlphabet;
    }

    public void write(DataOutput out) throws IOException {
        record.write(out);
        isLatinAlphabet.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        record.readFields(in);
        isLatinAlphabet.readFields(in);
    }


    @Override
    public String toString() {
        return record.toString() + "\t" + isLatinAlphabet.toString();
    }


}
