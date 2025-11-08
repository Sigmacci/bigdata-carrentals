package org.example.carrentals;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;

import java.io.FileNotFoundException;

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Main(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Main");
        job.setJarByClass(this.getClass());

        Path input = new Path(args[0]);
        if (!input.getFileSystem(getConf()).exists(input)) {
            throw new FileNotFoundException(args[0]);
        }

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(CarRentalMapper.class);
        job.setCombinerClass(CarRentalCombiner.class);
        job.setReducerClass(CarRentalReducer.class);

        job.setMapOutputKeyClass(CarYear.class);
        job.setMapOutputValueClass(TotalCompleted.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}