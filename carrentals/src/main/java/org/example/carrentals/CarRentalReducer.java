package org.example.carrentals;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CarRentalReducer extends Reducer<CarYear, TotalCompleted, Text, Text> {
    private final Text result = new Text();
    double ratio;

    @Override
    public void reduce(CarYear key, Iterable<TotalCompleted> values, Context context) throws IOException, InterruptedException {
        ratio = 0d;
        final TotalCompleted totalCompleted = new TotalCompleted();
        for (TotalCompleted t : values) {
            totalCompleted.addTotalCompleted(t);
        }

        ratio = (double) totalCompleted.completed.get() / totalCompleted.total.get();
        result.set(new Text(String.format("%d\t%f", totalCompleted.total.get(), ratio)));
        context.write(new Text(key.toString()), result);
    }
}
