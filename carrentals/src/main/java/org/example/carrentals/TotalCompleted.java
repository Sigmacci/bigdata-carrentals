package org.example.carrentals;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TotalCompleted implements WritableComparable<TotalCompleted> {

    IntWritable total;
    IntWritable completed;

    public TotalCompleted() {
        set(new  IntWritable(0), new IntWritable(0));
    }

    public TotalCompleted(int total, int completed) {
        set(new IntWritable(total), new IntWritable(completed));
    }

    public void set(IntWritable total, IntWritable completed) {
        this.total = total;
        this.completed = completed;
    }

    public void addTotalCompleted(TotalCompleted t) {
        set(new IntWritable(this.total.get() + t.total.get()), new IntWritable(this.completed.get() + t.completed.get()));
    }

    @Override
    public int compareTo(TotalCompleted o) {
        int comparison = total.compareTo(o.total);
        if (comparison == 0) {
            comparison = completed.compareTo(o.completed);
        }
        return comparison;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        total.write(dataOutput);
        completed.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        total.readFields(dataInput);
        completed.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TotalCompleted that = (TotalCompleted) o;
        return total.equals(that.total) && completed.equals(that.completed);
    }
}
