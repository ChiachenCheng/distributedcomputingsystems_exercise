package DSPPCode.mapreduce.average_score.impl;

import DSPPCode.mapreduce.average_score.question.ScoreMapper;
import DSPPCode.mapreduce.average_score.question.Util;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

public class ScoreMapperImpl extends ScoreMapper {

    private final Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            String str = itr.nextToken();
            String[] strs = str.split(",");
            int l = strs.length;
            for (int i = 1; i < l; i++){
                String course = Util.getCourseName(i-1);
                word.set(course);
                try {
                    int n = Integer.parseInt(strs[i]);
                    IntWritable num = new IntWritable(n);
                    context.write(word, num);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }

            }
        }
    }
}
