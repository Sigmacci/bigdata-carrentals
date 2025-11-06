package org.example.carrentals;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CarYear implements WritableComparable<CarYear> {

    Text carId;
    Text year;

    public CarYear() {
        set(new Text(""), new Text(""));
    }

    public CarYear(Text carId, Text year) {
        set(carId, year);
    }

    public void set(Text carId, Text year) {
        this.carId = carId;
        this.year = year;
    }

    public Text getCarId() {
        return carId;
    }

    public Text getYear() {
        return year;
    }

    @Override
    public int compareTo(CarYear carYear) {
        int comparison = carId.compareTo(carYear.carId);

        if (comparison == 0) {
            comparison = year.compareTo(carYear.year);
        }

        return comparison;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        carId.write(dataOutput);
        year.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        carId.readFields(dataInput);
        year.readFields(dataInput);
    }

    @Override
    public String toString() {
        return this.carId.toString() + "," + this.year.toString() + ",";
    }
}
