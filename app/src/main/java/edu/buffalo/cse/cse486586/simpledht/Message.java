package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.HashMap;

/**
 * Created by manasitamboli on 4/1/18.
 */

public class Message implements Serializable {
        private static final long serialVersionUID = 1L;
        public String option;
        public String key;
        public String value;
        public int prev;
        public int next;
        public HashMap<String, String> msgMap = new HashMap<String, String>();

        public Message(){}

        public Message(String opt, String k, String v, int p, int s, HashMap<String, String> m) {
            option = opt;
            key = k;
            value = v;
            prev = p;
            next = s;
            msgMap = m;
        }
}
