package DSPPCode.spark.topk_power.impl;


import DSPPCode.spark.topk_power.question.TopKPower;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TopKPowerImpl extends TopKPower {

  @Override
  public int topKPower(JavaRDD<String> lines) {
    JavaPairRDD<Integer, Integer> mps = lines.flatMap(
        new FlatMapFunction<String, String>() {
          @Override
          public Iterator<String> call(String s) throws Exception {
            return Arrays.asList(s.split(" ")).iterator();
          }
        }
    ).mapToPair(
        new PairFunction<String, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(String s) throws Exception {
            return new Tuple2<Integer, Integer>(Integer.parseInt(s), 1);
          }
        }
    );
    JavaPairRDD<Integer, Integer> ans = mps.groupByKey().mapToPair(
        new PairFunction<Tuple2<Integer, Iterable<Integer>>, Integer, Integer>() {
          @Override
          public Tuple2<Integer, Integer> call(Tuple2<Integer, Iterable<Integer>> t) throws Exception {
            Integer s = 0;
            for(Integer i : t._2()){
              s += i;
            }
            return new Tuple2<Integer, Integer>(s * s, t._1());
          }
        }
    ).sortByKey(false);
    ans.foreach(t -> System.out.println(t._1 + " " + t._2));
    List<Tuple2<Integer, Integer>> a = ans.take(5);
    Integer ret = 0;
    for (Tuple2<Integer, Integer> t:a){
      ret += t._1();
    }
    return ret;
  }
}
