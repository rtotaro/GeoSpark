package org.datasyslab.geospark.geometryObjects;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;
import java.io.Serializable;

public class JavaSpatialIndexSerde extends Serializer {

    @Override
    public void write(Kryo kryo, Output output, Object o) {
        if (o instanceof Serializable) {
            Serializable s = (Serializable) o;
            byte[] bytes = SerializationUtils.serialize(s);
            output.write(bytes);
        } else {
            throw new IllegalArgumentException("Not serializable object " + o.toString());
        }
    }

    @Override
    public Object read(Kryo kryo, Input input, Class aClass) {
        try {
            byte[] objectData = new byte[input.available()];
            input.read(objectData);
            Object deserialize = SerializationUtils.deserialize(objectData);
            return deserialize;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
