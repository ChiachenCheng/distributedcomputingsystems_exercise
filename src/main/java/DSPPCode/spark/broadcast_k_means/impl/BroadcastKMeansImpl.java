package DSPPCode.spark.broadcast_k_means.impl;

import DSPPCode.spark.broadcast_k_means.question.BroadcastKMeans;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import java.util.List;

public class BroadcastKMeansImpl extends BroadcastKMeans {

  @Override
  public Integer closestPoint(List<Integer> p, Broadcast<List<List<Double>>> kPoints) {
    List<List<Double>> kPointsValue = kPoints.value();
    Double mindis = Double.MAX_VALUE;
    Integer lj = 0;
    int lk = kPointsValue.size();
    for (int j = 0; j < lk; j++){
      List<Double> kPoint = kPointsValue.get(j);
      int l = kPoint.size();
      Double dis = 0.0;
      for(int i = 0; i < l; i++){
        dis += Math.pow(kPoint.get(i)-p.get(i),2);
      }
      if(dis < mindis){
        mindis = dis;
        lj = j;
      }
    }
    return lj;
  }

  @Override
  public Broadcast<List<List<Double>>> createBroadcastVariable(JavaSparkContext sc,
      List<List<Double>> localVariable) {
    Broadcast<List<List<Double>>> broadcast = sc.broadcast(localVariable);
    return broadcast;
  }
}
