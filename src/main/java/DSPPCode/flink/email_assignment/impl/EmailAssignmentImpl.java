package DSPPCode.flink.email_assignment.impl;

import DSPPCode.flink.email_assignment.question.EmailAssignment;
import DSPPCode.flink.email_assignment.question.Request;
import DSPPCode.flink.email_assignment.question.RequestType;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import java.util.HashMap;

public class EmailAssignmentImpl extends EmailAssignment{

  @Override
  public DataStream<String> processRequest(DataStream<Request> requests) {
    DataStream<String> ans = requests.map(
        new MapFunction<Request, String>() {
          HashMap<String, Integer> hash = new HashMap<String, Integer>();
          @Override
          public String map(Request request) throws Exception {
            String TRUE = "SUCCESS";
            String FALSE = "FAILURE";
            String regex = "^[a-z0-9A-Z_]+$";
            if (!request.getAlias().matches(regex)) {
              // System.out.println("no bother");
              return FALSE;
            }
            Integer fi = new Integer(request.getDepart().getFirstLevelCode());
            Integer se = new Integer(request.getDepart().getSecondLevelCode());
            String k = fi.toString() + se.toString() + request.getAlias();
            Integer rid = hash.getOrDefault(k, -1);
            if (rid == -1 && request.getType() == RequestType.APPLY){
              hash.put(k, request.getId());
              System.err.println(hash);
              return TRUE;
            }
            else if (rid == request.getId() && request.getType() == RequestType.REVOKE){
              hash.put(k, -1);
              System.out.println(hash);
              return TRUE;
            }
            else {
              // System.out.println(rid);
              // System.out.println(request.getId());
              return FALSE;
            }
          }
        }
    );
    DataStream<String> test = ans.map(
        new MapFunction<String, String>() {
          @Override
          public String map(String s) throws Exception {
            System.err.println(s);
            return s;
          }
        }
    );
    return ans;
  }
}
