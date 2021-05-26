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
            if (!request.getAlias().matches(regex))
              return FALSE;
            System.out.println(hash);
            String k = request.getDepart().toString() + request.getAlias();
            Integer rid = hash.getOrDefault(k, -1);
            if (rid == -1 && request.getType() == RequestType.APPLY){
              hash.put(k, request.getId());
              return TRUE;
            }
            else if (rid == request.getId() && request.getType() == RequestType.REVOKE){
              hash.put(k, -1);
              return TRUE;
            }
            else return FALSE;
          }
        }
    );
    ans.print();
    return ans;
  }
}
