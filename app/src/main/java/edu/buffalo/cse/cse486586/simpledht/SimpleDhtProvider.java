/* REFERENCES :
Assignment PA-2
https://developer.android.com/index.html
https://docs.oracle.com/javase/tutorial/
http://www.tutorialspoint.com/java/
https://www.tutorialspoint.com/android/index.htm
https://alvinalexander.com/java/jwarehouse/android-examples/samples/android-8/SearchableDictionary/src/com/example/android/searchabledict/DictionaryDatabase.java.shtml
https://docs.oracle.com/javase/7/docs/api/java/util/HashMap.html
https://www.java-examples.com/
http://www.devmanuals.com/tutorials/java/corejava/index.html
http://www.informit.com/articles/article.aspx?p=101766&seqNum=9
https://docs.oracle.com/javase/specs/jls/se7/html/jls-4.html#jls-4.2.3
https://developer.android.com/reference/android/database/MatrixCursor.html
https://medium.com/@xabaras/creating-a-cursor-from-a-list-with-matrixcursor-ab71877ecf2c
https://www.programcreek.com/java-api-examples/android.database.MatrixCursor
http://blogs.innovationm.com/multiple-asynctask-in-android/
https://developer.android.com/reference/android/os/AsyncTask.html
https://dzone.com/articles/two-ways-convert-java-map
https://docs.oracle.com/javase/7/docs/api/java/lang/StringBuilder.html
https://docs.oracle.com/javase/tutorial/essential/concurrency/guardmeth.html
https://www.programcreek.com/2009/02/notify-and-wait-example/
https://www.cs.helsinki.fi/webfm_send/1041
https://pdos.csail.mit.edu/papers/chord:tyan-meng.pdf
https://pdos.csail.mit.edu/papers/chord:tburkard-meng.pdf
 */

package edu.buffalo.cse.cse486586.simpledht;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map.Entry;

public class SimpleDhtProvider extends ContentProvider {
    private static final String TAG = SimpleDhtProvider.class.getSimpleName();
    private static final int SERVER_PORT = 10000;
    private String myNode, prevNode, nextNode;
    private int myPort, prevPort, nextPort;
    private final Message msgObj = new Message(null,null, null, 0, 0, emptyMap);
    private static HashMap<String, String> emptyMap = new HashMap<String, String>();
    private static HashMap<String, String> finalMap = new HashMap<String, String>();

    @Override
    public boolean onCreate() {
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(
                Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.parseInt(portStr) * 2;
        try {
            //Log.d("test", "My node to hash : " + portStr);
            myNode = genHash(portStr);
            prevNode = myNode;
            nextNode = myNode;
            prevPort = myPort;
            nextPort = myPort;
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Can't generate the node id:\n" + e.getMessage());
            return false;
        }
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket:\n" + e.getMessage());
            return false;
        }
        //Log.d("test", "Nodes ports init : " + myNode+"-"+myPort+"-"+prevNode+"-"+nextNode+"-"+prevPort+"-"+nextPort);
        //Log.d("test", "Params before oncreate join : " + myPort + " : " + myNode);
        //Log.d("test", "See empty map val : "+ emptyMap);
        if (myPort != 11108) {
            String param = "JOIN" + "-" + myNode + "-" + null + "-" + String.valueOf(myPort) + "-" + String.valueOf(myPort) + "-" + null + "-" + String.valueOf(11108);
            //Log.d("test", "client task 1 params : " + param);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
        }
        Log.d("test", "joined on create success!");
        return true;
    }

    private void join(Message msg) {
        //Log.d("test", "join myport : " + myPort);
        //Log.d("test", "join prevport : " + prevPort);
        if (ringCheck(msg.key)) {
            if (myPort != prevPort) {
                //Log.d("test", "not same node : get next");
                String param_next = "GETNEXT" + "-" + msg.key + "-" + null + "-" + "0" + "-" + String.valueOf(msg.next) + "-" + null + "-" + String.valueOf(prevPort);
                //Log.d("test", "get next params : " + param_next);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param_next);
                //Log.d("test", "get next params : " + prevPort+ msg.key+ null+ "0"+ msg.next);
                //Log.d("test", "get next at join 1st");
                String param_final = "ACK" + "-" + prevNode + "-" + myNode + "-" + String.valueOf(prevPort) + "-" + String.valueOf(myPort) + "-" + null + "-" + String.valueOf(msg.prev);
                //Log.d("test", "client task 2 join ok params : " + param_final);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param_final);
                //Log.d("test", "Ack at join 2nd");
                reinit("GETNEXTACK", msg);
            } else {
                //Log.d("test", "same node join");
                //Log.d("test", "finaljoin params : " + String.valueOf(msg.prev) + prevNode + nextNode + String.valueOf(prevPort) + String.valueOf(nextPort));
                String param_ack = "ACK" + "-" + prevNode + "-" + nextNode + "-" + String.valueOf(prevPort) + "-" + String.valueOf(nextPort) + "-" + null + "-" + String.valueOf(msg.prev);
                //Log.d("test", "client task 2 join ok params : " + param_ack);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param_ack);
                //Log.d("test", "Ack join 1st");
                reinit("ACK", msg);
            }
        } else {
            //Log.d("test", "genhash starts failing");
            //Log.d("test", "next port after genhash failed : " + nextPort);
            String param_join = "JOIN" + "-" + msg.key + "-" + msg.value + "-" + String.valueOf(msg.prev) + "-" + String.valueOf(msg.next) + "-" + null + "-" + String.valueOf(nextPort);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param_join);
            //Log.d("test", "after client task genhash failed at join");
        }
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        try {
            if (ringCheck(genHash(key))) {
                finalMap.put(key, value);
                Log.d("test", "insert done in final map : " + finalMap);
            } else {
                String param = "INSERT" + "-" + key + "-" + value + "-" + "0" + "-" + "0" + "-" + null + "-" + String.valueOf(nextPort);
                //Log.d("test", "insert params : " + param_);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Can't generate insert hash:\n" + e.getMessage());
        }
        Log.v("insert", values.toString());
        return uri;
    }


    @Override
    public String getType(Uri uri) {
        return null;
    }


    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        boolean wait = true;
        HashMap<String, String> queryMap = new HashMap<String, String>();
        Log.d("test", "Main query function");
        if ((selection.equals("@")) || (selection.equals("*"))) {
            //Log.d("test", "start * or @ query");
            return selectQuery(selection, queryMap, myPort, wait);
        } else {
            //Log.d("test", "start normal query");
            return returnQuery(selection, myPort, wait);
        }
    }

    private Cursor selectQuery(String key, HashMap<String, String> queryMap, int queryPort,
                                boolean wait) {
        String[] column = new String[]{"key", "value"};
        MatrixCursor cr = new MatrixCursor(column);
        if (key.equals("*")) {
            Log.d("test", "* query starts");
            //Log.d("test", "* query final map : " + finalMap);
            //Log.d("test", "* query map before put : " + queryMap);
            queryMap.putAll(finalMap);
            //Log.d("test", "* query map : " + queryMap);
            String strMap = decomposeMap(queryMap);
            //Log.d("test", "str Map of * query : " + strMap);
            String param = "QUERYALL" + "-" + key + "-" + null + "-" + queryPort + "-" + "0" + "-" + strMap + "-" + String.valueOf(nextPort);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
            if (wait) {
                //Log.d("test", "map wait entered for map : " + queryMap);
                synchronized (msgObj) {
                    //Log.d("test", "map synch entered for map : " + queryMap);
                    //Log.d("test", "msgObj before wait : " + msgObj.msgMap);
                    try {
                        msgObj.wait();
                        //Log.d("test", "msgObj waiting for map : " + queryMap);
                        //Log.d("test", "msgObj before altering : " + msgObj.msgMap);
                    } catch (InterruptedException e) {
                        Log.e(TAG, "Get all interrupted");
                    }
                }
                //Log.d("test", "adding to msgObj map : " + queryMap);
                //Log.d("test", "msgObj before altering : " + msgObj.msgMap);
                for (Entry<String, String> pair : msgObj.msgMap.entrySet()) {
                    cr.addRow(new Object[]{pair.getKey(), pair.getValue()});
                    //Log.d("test", "* query mmessage map : " + msgObj.msgMap);
                }
            }
        } else if (key.equals("@")) {
            Log.d("test", "all query started");
            Log.d("test", "@ query starts");
            for (Entry<String, String> pair : finalMap.entrySet()) {
                cr.addRow(new Object[]{pair.getKey(), pair.getValue()});
                //Log.d("test", "@ query final map : " + finalMap);
            }
        }
        return cr;
    }


    private Cursor returnQuery(String key, int queryPort, boolean wait) {
        String[] column = new String[]{"key", "value"};
        MatrixCursor cr = new MatrixCursor(column);
        Log.d("test", "normal query function entered");
        try {
            if (ringCheck(genHash(key))) {
                String val = finalMap.get(key);
                if (myPort == queryPort) {
                    cr.addRow(new Object[]{key, val});
                    //Log.d("test", "normal query final map : " + finalMap);
                } else {
                    String param = "QUERY" + "-" + null + "-" + val + "-" + String.valueOf(queryPort) + "-" + "0" + "-" + null + "-" + String.valueOf(queryPort);
                    //Log.d("test", "Params for query : " + param);
                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
                }
                return cr;
            } else {
                String param = "QUERY" + "-" + key + "-" + null + "-" + String.valueOf(queryPort) + "-" + "0" + "-" + null + "-" + String.valueOf(nextPort);
                //Log.d("test", "Params : " + param);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
                if (wait) {
                    synchronized (msgObj) {
                        try {
                            msgObj.wait();
                        } catch (InterruptedException e) {
                            Log.e(TAG, "Query interrupted");
                        }
                    }
                    cr.addRow(new Object[]{key, msgObj.value});
                }
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Can't generate query key:\n" + e.getMessage());
        }
        return cr;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.d("test", "main del function entered : " + selection + myPort);
        if (selection.equals("@") || selection.equals("*")) {
            //Log.d("test", "selectdel calling");
            selectDel(selection, prevPort);
        } else {
            //Log.d("test", "normal del calling");
            returnDel(selection, myPort);
        }
        return 0;
    }

    private void selectDel(String key, int queryPort) {
        Log.d("test", "void del function entered");
        Log.d("test", "del on selection param");
        if (key.equals("*")) {
            //Log.d("test", "* del called");
            String param = "DELETE" + "-" + key + "-" + null + "-" + String.valueOf(queryPort) + "-" + "0" + "-" + null + "-" + String.valueOf(nextPort);
            //Log.d("test", "del params : "+param);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
        }
        else if (key.equals("@")) {
            //Log.d("test", "@ del called");
        finalMap.clear();
        Log.d("test", "map cleared! nextport and queryport : " + nextPort + "-" + queryPort);
        }
    }

    private void returnDel(String key, int queryPort) {
        Log.d("test", "normal del");
        try {
            if (ringCheck(genHash(key))) {
                finalMap.remove(key);
                //Log.d("test", "map after single del");
            } else {
                String param = "DELETE" + "-" + key + "-" + null + "-" + String.valueOf(queryPort) + "-" + "0" + "-" + null + "-" + String.valueOf(nextPort);
                //Log.d("test", "normal del params :"+param);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
            }
        } catch (NoSuchAlgorithmException e) {
            Log.e(TAG, "Can't generate delete hash:\n" + e.getMessage());
        }
    }


    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private boolean ringCheck(String key) {
            /* ringCheck condition for
            1. cur = prev
            2. prev > cur ->
            keynode > prev & keynode > cur
            3. prev > keynode ->
            cur  >= keynode
            4. keynode > prev ->
            cur >= keynode
             */
        BigInteger keynode = new BigInteger(key, 16);
        BigInteger cur = new BigInteger(myNode, 16);
        BigInteger prev = new BigInteger(prevNode, 16);
        if ((cur.compareTo(prev) == 0) ||
                ((prev.compareTo(cur) == 1) && (keynode.compareTo(prev) == 1) && (keynode.compareTo(cur) == 1)) ||
                ((prev.compareTo(cur) == 1) && (prev.compareTo(keynode) == 1) && ((cur.compareTo(keynode) == 1) || (cur.compareTo(keynode) == 0))) ||
                ((keynode.compareTo(prev) == 1) && ((cur.compareTo(keynode) == 1) || (cur.compareTo(keynode) == 0))))
            return true;
        else return false;
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private void reinit(String opt, Message obj) {
        if (opt.equals("ACK")) {
            prevNode = obj.key;
            prevPort = obj.prev;
            nextNode = obj.key;
            nextPort = obj.next;
        } else if (opt.equals("GETNEXT")) {
            nextNode = obj.key;
            nextPort = obj.next;
        }
        else if (opt.equals("GETNEXTACK")) {
            prevNode = obj.key;
            prevPort = obj.prev;
        }
    }

    private String decomposeMap(HashMap<String, String> map) {
        if (map.isEmpty()) {
            //Log.d("test", "null check entered in decompose : " + map);
            return null;
        } else {
            String[] keys = new String[map.size()];
            String[] values = new String[map.size()];
            int index = 0;
            for (Entry<String, String> pair : map.entrySet()) {
                keys[index] = pair.getKey();
                //Log.d("test", "decompose fn keys array : "+keys[index]);
                values[index] = pair.getValue();
                //Log.d("test", "decompose fn values array : "+values[index]);
                index++;
            }
            StringBuilder keysBuild = new StringBuilder();
            for (int i = 0; i < keys.length; i++) {
                keysBuild.append(keys[i]);
                keysBuild.append(" ");
            }
            String keysStr = keysBuild.toString();
            StringBuilder valuesBuild = new StringBuilder();
            for (int i = 0; i < values.length; i++) {
                valuesBuild.append(values[i]);
                valuesBuild.append("&");
            }
            String valuesStr = valuesBuild.toString();
            String sendMap = keysStr + "," + valuesStr;
            return sendMap;
        }
    }

    private boolean synch(String opt, Message obj) {
        if (myPort == obj.prev) {
            //Log.d("test", "multiple resource access true");
            if (opt.equals("QUERY")) {
                msgObj.value = obj.value;
                synchronized (msgObj) {
                    //Log.d("test", "synch on mobj for query");
                    msgObj.notify();
                }
            } else if (opt.equals("QUERYALL")) {
                msgObj.msgMap = obj.msgMap;
                synchronized (msgObj) {
                    //Log.d("test", "synch on mobj for queryall");
                    msgObj.notify();
                }
            }
            return true;
        } else
            return false;
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {
        Socket socket = null;
        ObjectOutputStream oos;

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                String[] msgToSend = msgs[0].split("-");
                //Log.d("test", "Check msgs arr length : "+msgToSend.length);
                //Log.d("test", "final node : " + msgToSend[6]);
                socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.valueOf(msgToSend[6]));
                //Log.d("test", "New client socket open");
                oos = new ObjectOutputStream(socket.getOutputStream());
                if (msgToSend[5].matches("null")) {
                    //Log.d("test", "msg null");
                    Message finalMsg = new Message(msgToSend[0], msgToSend[1], msgToSend[2],
                            Integer.valueOf(msgToSend[3]), Integer.valueOf(msgToSend[4]),
                            emptyMap);
                    //Log.d("test", "msg composed");
                    oos.writeObject(finalMsg);
                    //Log.d("test", "msg written");
                    oos.flush();

                } else {
                    //Log.d("test", "msg not null : " + msgToSend[5]);
                    String[] keyval = msgToSend[5].split(",");
                    //Log.d("test", "keyval 0 : " +keyval[0]);
                    //Log.d("test", "keyval 1 : " +keyval[1]);
                    String keysStr = keyval[0];
                    String valueStr = keyval[1];
                    String[] keys1 = keysStr.split(" ");
                    String[] values1 = valueStr.split("&");
                    //Log.d("test", "length of keys arr in client : " + keys1.length);
                    //Log.d("test", "length of values arr in client : " + values1.length);
                    HashMap<String, String> newMap = new HashMap<String, String>();
                    if (keys1.length == values1.length) {
                        for (int index = 0; index < keys1.length; index++) {
                            newMap.put(keys1[index], values1[index]);
                            //Log.d("test", "new map in client every time : " + newMap);
                        }
                    }
                    //Log.d("test", "new map in client : " + newMap);
                    Message finalMsg = new Message(msgToSend[0], msgToSend[1], msgToSend[2],
                            Integer.valueOf(msgToSend[3]), Integer.valueOf(msgToSend[4]),
                            newMap);
                    oos.writeObject(finalMsg);
                    Log.d("test", "Client msg written");
                    oos.flush();

                }
            } catch (UnknownHostException e) {
                e.printStackTrace();
            } catch (SocketException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            //DataInputStream dis = null;
            ObjectInputStream ois;
            Socket socket = null;
            Uri uri = null;
            //Log.d("test", "Server doinbackground started");
            try {
                while (true) {
                    socket = serverSocket.accept();
                    ois = new ObjectInputStream(socket.getInputStream());
                    Message msgReceived = (Message) ois.readObject();
                    if (msgReceived.option.equals("JOIN")) {
                        Log.d("test", "joining from server");
                        join(msgReceived);
                        Log.d("test", "join success!");

                    } else if (msgReceived.option.equals("ACK")) {
                        //Log.d("test", "ack before : " + msgReceived.key + msgReceived.value + msgReceived.prev + msgReceived.next);
                        reinit("ACK", msgReceived);
                        //Log.d("test", "ack after : " + prevNode + nextNode + prevPort + nextPort);
                        Log.d("test", "ack success!");
                        
                    } else if (msgReceived.option.equals("GETNEXT")) {
                        //Log.d("test", "check next before : " + msgReceived.key + msgReceived.next);
                        reinit("GETNEXT", msgReceived);
                        //Log.d("test", "check next after : " + nextNode + nextPort);
                        Log.d("test", "next node success!");

                    } else if (msgReceived.option.equals("INSERT")) {
                        uri = buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider");
                        ContentValues cntval = new ContentValues();
                        cntval.put("key", msgReceived.key);
                        cntval.put("value", msgReceived.value);
                        getContext().getContentResolver().insert(uri, cntval);
                        //publishProgress(msgReceived.value);
                        Log.d("test", "insert success!");

                    } else if (msgReceived.option.equals("QUERY")) {
                        Log.d("test", "query started!");
                        if (!synch("QUERY", msgReceived)) {
                            returnQuery(msgReceived.key, msgReceived.prev, false);
                            Log.d("test", "query success!");
                        }

                    } else if (msgReceived.option.equals("QUERYALL")) {
                        Log.d("test", "queryall started!");
                        if (!synch("QUERYALL", msgReceived)) {
                            selectQuery(msgReceived.key, msgReceived.msgMap, msgReceived.prev, false);
                            Log.d("test", "queryall success!");
                        }

                    } else if (msgReceived.option.equals("DELETE")) {
                        //Log.d("tag", "del server call : " + msgReceived.key + msgReceived.prev);
                        returnDel(msgReceived.key, msgReceived.prev);
                        Log.d("test", "delete success!");

                    }
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException:\n" + e.getMessage());
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "ServerTask ObjectInputStream ClassNotFoundException");
            }
            return null;
        }
    }
}