package utils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableNetPerformMap implements Writable {

    private LongWritable numberOfBytes;
    private LongWritable extractionTime;

    public WritableNetPerformMap() {
        numberOfBytes = new LongWritable(0);
        extractionTime = new LongWritable();
    }

    public WritableNetPerformMap(LongWritable numberOfBytes, LongWritable extractionTime) {
        this.numberOfBytes = numberOfBytes;
        this.extractionTime = extractionTime;
    }

    public LongWritable getNumberOfBytes() {
        return numberOfBytes;
    }

    public void setNumberOfBytes(LongWritable numberOfBytes) {
        this.numberOfBytes = numberOfBytes;
    }

    public LongWritable getExtractionTime() {
        return extractionTime;
    }

    public void setExtractionTime(LongWritable extractionTime) {
        this.extractionTime = extractionTime;
    }

    public void write(DataOutput out) throws IOException {
        numberOfBytes.write(out);
        extractionTime.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        numberOfBytes.readFields(in);
        extractionTime.readFields(in);
    }


    @Override
    public String toString() {
        return extractionTime.toString() + "\t" + numberOfBytes.toString();
    }

}



