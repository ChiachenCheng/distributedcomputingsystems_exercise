package DSPPCode.mapreduce.average_score.impl;

import DSPPCode.mapreduce.average_score.question.ScoreReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class ScoreReducerImpl extends ScoreReducer{

    private IntWritable result = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        int sum = 0, num = 0;
        for (IntWritable val : values) {
            sum += val.get();
            num += 1;
        }
        int avg = sum/num;
        result.set(avg);
        context.write(key, result);
    }
}
