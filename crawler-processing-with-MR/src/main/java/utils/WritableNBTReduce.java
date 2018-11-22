package utils;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class WritableNBTReduce implements Writable {

    private LongWritable numberOfBytes;
    private LongWritable extractionTime;
    private DoubleWritable bandwith;

    public WritableNBTReduce() {
        numberOfBytes = new LongWritable(0);
        extractionTime = new LongWritable(0);
        bandwith = new DoubleWritable(0);
    }

    public WritableNBTReduce(LongWritable numberOfBytes, LongWritable extractionTime, DoubleWritable bandwith) {
        this.numberOfBytes = numberOfBytes;
        this.extractionTime = extractionTime;
        this.bandwith = bandwith;
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

    public DoubleWritable getBandwith() {
        return bandwith;
    }

    public void setBandwith(DoubleWritable bandwith) {
        this.bandwith = bandwith;
    }

    public void write(DataOutput out) throws IOException {
        numberOfBytes.write(out);
        extractionTime.write(out);
        bandwith.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        numberOfBytes.readFields(in);
        extractionTime.readFields(in);
        bandwith.readFields(in);
    }


    @Override
    public String toString() {
        return extractionTime.toString() + "\t" + numberOfBytes.toString() + "\t" + bandwith.toString();
    }

}



