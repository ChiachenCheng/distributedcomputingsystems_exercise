package DSPPCode.mapreduce.pagerank.impl;

import DSPPCode.mapreduce.pagerank.question.PageRankMapper;
import DSPPCode.mapreduce.pagerank.question.ReducePageRankWritable;
import DSPPCode.mapreduce.pagerank.question.PageRankRunner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class PageRankMapperImpl extends PageRankMapper {

    private Map<String, String> rankTable = new HashMap<>();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /* 步骤2：编写处理逻辑将[K1,V1]转换为[K2,V2]并输出 */
        if (PageRankRunner.iteration == 0 && rankTable.isEmpty()) {
            URI uri = context.getCacheFiles()[0];
            FileSystem fs = FileSystem.get(uri, new Configuration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
            String content;
            while ((content = reader.readLine()) != null) {
                String[] datas = content.split(" ");
                rankTable.put(datas[0], datas[1]);
            }
        }

        if (PageRankRunner.iteration == 0) {
            // 以空格为分隔符切分
            String[] pageInfo = value.toString().split(" ");
            // 网页的排名值
            double pageRank = Double.parseDouble(rankTable.get(pageInfo[0]));
            // 网页的出站链接数
            int outLink = pageInfo.length - 1;
            ReducePageRankWritable writable;
            writable = new ReducePageRankWritable();
            // 计算贡献值并保存
            writable.setData(String.valueOf(pageRank / outLink));
            // 设置对应标识
            writable.setTag(ReducePageRankWritable.PR_L);
            // 对于每一个出站链接，输出贡献值
            for (int i = 1; i < pageInfo.length; i += 1) {
                context.write(new Text(pageInfo[i]), writable);
            }
            writable = new ReducePageRankWritable();
            // 保存网页信息并标识
            writable.setData(value.toString());
            writable.setTag(ReducePageRankWritable.PAGE_INFO);
            // 以输入的网页信息的网页名称为key进行输出
            context.write(new Text(pageInfo[0]), writable);
        }

        if(PageRankRunner.iteration != 0) {
            String[] pageInfo = value.toString().split(" ");
            // 网页的出站链接数
            int outLink = pageInfo.length - 2;
            // 网页的排名值
            double pageRank = Double.parseDouble(pageInfo[outLink + 1]);
            ReducePageRankWritable writable;
            writable = new ReducePageRankWritable();
            // 计算贡献值并保存
            writable.setData(String.valueOf(pageRank / outLink));
            // 设置对应标识
            writable.setTag(ReducePageRankWritable.PR_L);
            // 对于每一个出站链接，输出贡献值
            for (int i = 1; i < pageInfo.length - 1; i += 1) {
                context.write(new Text(pageInfo[i]), writable);
            }
            writable = new ReducePageRankWritable();
            // 保存网页信息并标识
            writable.setData(value.toString());
            writable.setTag(ReducePageRankWritable.PAGE_INFO);
            // 以输入的网页信息的网页名称为key进行输出
            context.write(new Text(pageInfo[0]), writable);
        }
    }
}

