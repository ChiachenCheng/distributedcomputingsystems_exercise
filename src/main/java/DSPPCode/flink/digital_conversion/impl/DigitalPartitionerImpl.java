package DSPPCode.flink.digital_conversion.impl;

import DSPPCode.flink.digital_conversion.question.DigitalPartitioner;

public class DigitalPartitionerImpl<T> extends DigitalPartitioner<T>{

  public int partition(T key, int numPartitions){
    if(key.toString().charAt(0) < '5')
      return 0;
    else return 1;
  }

}