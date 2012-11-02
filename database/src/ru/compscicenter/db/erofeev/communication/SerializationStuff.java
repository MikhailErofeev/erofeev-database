package ru.compscicenter.db.erofeev.communication;

import java.io.*;

/**
 * Created with IntelliJ IDEA.
 * User: erofeev
 * Date: 10/26/12
 * Time: 11:06 AM
 */
public class SerializationStuff {
    public static byte[] getBytes(Serializable data) {
        ObjectOutput out = null;
        byte[] res = null;
        if (data instanceof String) {
            return ((String) data).getBytes();
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(data);
            res = bos.toByteArray();
        } catch (IOException e) {
            return null;
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
                bos.close();
            } catch (IOException e) {

            }
        }
        return res;
    }

    public static String getStringFromException(Exception e){
        StringBuilder exception = new StringBuilder();
        for (StackTraceElement ste : e.getStackTrace()) {
            exception.append(ste.toString() + "\n");
        }
        return exception.toString();
    }

    public static Serializable getObject(byte[] data) {
        if (data == null){
            return null;
        }
        ObjectInputStream readS = null;
        Serializable c = null;
        try {
            readS = new ObjectInputStream(new ByteArrayInputStream(data));
            c = (Serializable) readS.readObject();
            //@TODO узкое место. В базу можно заслать неизвестный класс, и тогда наступит боль
        } catch (ClassNotFoundException e) {
            return null;
        } catch (StreamCorruptedException e) {
            return new String(data);
        } catch (IOException e) {
            return null;
        }
        if (c instanceof byte[]) {
            return new String((byte[]) c);
        } else {
            return c;
        }
    }
}
