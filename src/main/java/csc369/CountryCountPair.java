package csc369;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.*;

public class CountryCountPair implements Writable, WritableComparable<CountryCountPair> {
    private Text country = new Text();
    private IntWritable count = new IntWritable();
    
    public CountryCountPair() {
    }

    public CountryCountPair(String country, int count) {
        this.country.set(country);
        this.count.set(count);
    }

    @Override
    public void write(DataOutput out) throws IOException{
        country.write(out);
        count.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        country.readFields(in);
        count.readFields(in);
    }
    
    @Override
    public int compareTo(CountryCountPair other) {
        int cmp = country.compareTo(other.country);
        if (cmp != 0) {
            return cmp;
        }
        return count.compareTo(other.count);
    }

    public Text getCountry() {
        return country;
    }

    public IntWritable getCount() {
        return count;
    }
}
