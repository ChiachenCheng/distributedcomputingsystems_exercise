package DSPPCode.mapreduce.max_temperature.impl;

import DSPPCode.mapreduce.max_temperature.question.MaxTemperatureReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class MaxTemperatureReducerImpl extends MaxTemperatureReducer {

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int max = 0;
        for (IntWritable val : values) {
            max = max > val.get() ? max : val.get();
        }
        result.set(max);
        context.write(key, result);
    }

}