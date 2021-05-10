package DSPPCode.spark.pagerank_top3.impl;

import DSPPCode.spark.pagerank_top3.question.PageRankTop3;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
public class PageRankTop3Impl extends PageRankTop3{

  /**
   * TODO 请完成该方法
   * <p>
   * 请在此方法中计算出按rank值排名前三名的(id,rank)键值对
   *
   * @param text       包含了输入文本文件数据的RDD
   * @param iterateNum 迭代轮数
   * @return 前三名节点的  (网页名称, 该网页 rank 值)  键值对
   */
  public JavaPairRDD<String, Double> getTop3(JavaRDD<String> text, int iterateNum){
    /* 步骤2：按应用逻辑使用操作算子编写DAG，其中包括RDD的创建、转换和行动等 */
    double factor = 0.85; // 指定系数

    // 将文本数据转换成[网页, {链接列表}]键值对
    JavaPairRDD<String, List<String>> links =
        text.mapToPair(
            new PairFunction<String, String, List<String>>() {
              @Override
              public Tuple2<String, List<String>> call(String line) throws Exception {
                String[] tokens = line.split(" ");
                List<String> list = new ArrayList<>();
                for (int i = 2; i < tokens.length; i+=2) {
                  list.add(tokens[i]);
                }
                return new Tuple2<>(tokens[0], list);
              }
            })
            .cache(); // 持久化到内存

    long N = text.count(); // 获取网页总数N // 該步使用算法要求行數嚴格等於節點數

    // 初始化每个页面的排名值，得到[网页, 排名值]键值对
    JavaPairRDD<String, Double> ranks =
        text.mapToPair(
            new PairFunction<String, String, Double>() {
              @Override
              public Tuple2<String, Double> call(String line) throws Exception {
                String[] tokens = line.split(" ");
                return new Tuple2<>(tokens[0], Double.valueOf(tokens[1]));
              }
            });

    // 执行iterateNum次迭代计算
    for (int iter = 1; iter <= iterateNum; iter++) {
      JavaPairRDD<String, Double> contributions =
          links
              // 将links和ranks做join，得到[网页, {{链接列表}, 排名值}]
              .join(ranks)
              // 计算出每个网页对其每个链接网页的贡献值
              .flatMapToPair(
                  new PairFlatMapFunction<
                      Tuple2<String, Tuple2<List<String>, Double>>, String, Double>() {
                    @Override
                    public Iterator<Tuple2<String, Double>> call(
                        Tuple2<String, Tuple2<List<String>, Double>> t) throws Exception {
                      List<Tuple2<String, Double>> list = new ArrayList<>();
                      for (int i = 0; i < t._2._1.size(); i++) {
                        // 网页排名值除以链接总数
                        list.add(new Tuple2<>(t._2._1.get(i), t._2._2 / t._2._1.size()));
                      }
                      return list.iterator();
                    }
                  });

      ranks =
          contributions
              // 聚合对相同网页的贡献值，求和得到对每个网页的总贡献值
              .reduceByKey(
                  new Function2<Double, Double, Double>() {
                    @Override
                    public Double call(Double r1, Double r2) throws Exception {
                      return r1 + r2;
                    }
                  })
              // 根据公式计算得到每个网页的新排名值
              .mapValues(
                  new Function<Double, Double>() {
                    @Override
                    public Double call(Double v) throws Exception {
                      return (1 - factor) * 1.0 / N + factor * v;
                    }
                  }).sortByKey();
    }

    JavaPairRDD<String, Double> sranks = ranks
        .mapToPair((row)->new Tuple2<>(row._2,row._1))
        .sortByKey(false)
        .mapToPair((row)->new Tuple2<>(row._2,row._1));

    Double lim = sranks.take(3).get(2)._2;

    JavaPairRDD<String, Double> ans = sranks
        .filter(new Function<Tuple2<String, Double>, Boolean>() {
          @Override
          public Boolean call(Tuple2<String, Double> stringDoubleTuple2) throws Exception {
            return stringDoubleTuple2._2 >= lim;
          }
        }); // 該步使用算法要求不能有重複的PageRank值

    // 对排名值保留5位小数，并打印最终网页排名结果
    ans.foreach(new VoidFunction<Tuple2<String, Double>>() {
      @Override
      public void call(Tuple2<String, Double> t) throws Exception {
        System.out.println(t._1 + " " + String.format("%f", t._2));
      }
    });

    return ans;
  }
}