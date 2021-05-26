package DSPPCode.flink.digital_conversion.impl;

import DSPPCode.flink.digital_conversion.question.DigitalPartitioner;

public class DigitalPartitionerImpl extends DigitalPartitioner{

  public int partition(String key, int numPartitions){
    if(key.charAt(0) < '5')
      return 0;
    else return 1;
  }

}