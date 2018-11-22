package utils;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WritableTop10LargestMap {

    private LongWritable numberOfBytes;
    private LongWritable extractionTime;
    private DoubleWritable bandwith;
    private Text siteURI;

    public WritableTop10LargestMap() {
        numberOfBytes = new LongWritable(0);
        extractionTime = new LongWritable(0);
        bandwith = new DoubleWritable(0);
        siteURI = new Text();
    }

    public WritableTop10LargestMap(LongWritable numberOfBytes, LongWritable extractionTime, DoubleWritable bandwith, Text siteURI) {
        this.numberOfBytes = numberOfBytes;
        this.extractionTime = extractionTime;
        this.bandwith = bandwith;
        this.siteURI = siteURI;
    }

    public WritableTop10LargestMap(Text siteURI, WritableNetPerformReduce nbtReduce) {
        this.numberOfBytes = nbtReduce.getNumberOfBytes();
        this.extractionTime = nbtReduce.getExtractionTime();
        this.bandwith = nbtReduce.getBandwith();
        this.siteURI = siteURI;
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

    public Text getSiteURI() {
        return siteURI;
    }

    public void setSiteURI(Text siteURI) {
        this.siteURI = siteURI;
    }

    public void write(DataOutput out) throws IOException {
        numberOfBytes.write(out);
        extractionTime.write(out);
        bandwith.write(out);
        siteURI.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        numberOfBytes.readFields(in);
        extractionTime.readFields(in);
        bandwith.readFields(in);
        siteURI.readFields(in);

    }


    @Override
    public String toString() {
        return extractionTime.toString() + "\t" + numberOfBytes.toString() + "\t" +
                bandwith.toString() + "\t" + siteURI.toString();
    }

}



