package DSPPCode.mapreduce.max_temperature.impl;

import DSPPCode.mapreduce.max_temperature.question.MaxTemperatureMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class MaxTemperatureMapperImpl extends MaxTemperatureMapper {

    private static final IntWritable ONE = new IntWritable(1);

    private final Text word = new Text();
    private final Text word2 = new Text();

    private final Pattern pattern = Pattern.compile("\\W+");

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String str = itr.nextToken();
            str = pattern.matcher(str).replaceAll("");
            word.set(str);
            str = itr.nextToken();
            str = pattern.matcher(str).replaceAll("");
            int n = 0;
            try {
                n = Integer.parseInt(str);
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
            IntWritable tmp = new IntWritable(n);
            context.write(word, tmp);
        }
    }

}
