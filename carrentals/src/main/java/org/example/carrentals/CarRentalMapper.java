package org.example.carrentals;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CarRentalMapper extends Mapper<LongWritable, Text, CarYear, TotalCompleted> {

    private final CarYear carYear = new CarYear();
    private final TotalCompleted totalCompleted = new TotalCompleted();

    private final IntWritable one = new IntWritable(1);

    private final String regexp = "^\\S+,(CAR\\d+),CUST\\d+,(\\d+)-\\S+,\\S+,\\S+,(\\w+)$";

    @Override
    public void map(LongWritable offset, Text lineText, Context context) {
        try {
            if (offset.get() != 0) {
                Pattern pattern = Pattern.compile(regexp, Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(lineText.toString());

                if (matcher.find()) {
                    String carId = matcher.group(1);
                    String year = matcher.group(2);
                    String status = matcher.group(3);

                    carYear.set(new Text(carId), new Text(year));
                    totalCompleted.set(one, status.equals("Completed") ? one : new IntWritable(0));

                    context.write(carYear, totalCompleted);
                } else {
                    throw new RuntimeException("Invalid format in line: " + lineText.toString());
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
