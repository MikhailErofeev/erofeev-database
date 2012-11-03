package ru.compscicenter.db.erofeev.communication;

import java.io.*;
import java.util.logging.Logger;

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

    public static String getStringFromException(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString(); // stack trace as a string
    }

    public static Serializable getObject(byte[] data) {
        if (data == null) {
            return null;
        }
        ObjectInputStream readS = null;
        Serializable c = null;
        try {
            readS = new ObjectInputStream(new ByteArrayInputStream(data));
            c = (Serializable) readS.readObject();
        } catch (ClassNotFoundException e) {
            return null;
        } catch (StreamCorruptedException e) {
            return new String(data);
        } catch (IOException e) {
            if (data.length < 5){ //@TODO какая-то магия, 3-буквенные слова вываливались с IOE
                return new String(data);
            }else{
                return null;
            }
        }
        if (c instanceof byte[]) {
            return new String((byte[]) c);
        } else {
            return c;
        }
    }
}
