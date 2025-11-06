package org.example.carrentals;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarRentalCombiner extends Reducer<CarYear, TotalCompleted, CarYear, TotalCompleted> {
    private final TotalCompleted totalCompleted = new TotalCompleted(0, 0);

    @Override
    public void reduce(CarYear key, Iterable<TotalCompleted> values, Context context) throws IOException, InterruptedException {
        totalCompleted.set(new IntWritable(0), new IntWritable(0));
        for (TotalCompleted t : values) {
            totalCompleted.addTotalCompleted(t);
        }
        context.write(key, totalCompleted);
    }
}
