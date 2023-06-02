package org.example.serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.example.dro.Supplier;

import java.nio.ByteBuffer;
import java.util.Map;

public class SupplierSerializer implements Serializer<Supplier> {

    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // nothing to configure
    }

    @Override
    public byte[] serialize(String topic, Supplier data) {

        int sizeOfName;
        int sizeOfDate;
        byte[] serializedName;
        byte[] serializedDate;

        try {
            if (data == null)
                return new byte[0];

            serializedName = data.getName().getBytes(encoding);
            sizeOfName = serializedName.length;
            serializedDate = data.getStartDate().toString().getBytes(encoding);
            sizeOfDate = serializedDate.length;

            ByteBuffer buf = ByteBuffer.allocate(4 + 4 + sizeOfName + 4 + sizeOfDate);
            buf.putInt(data.getID());
            buf.putInt(sizeOfName);
            buf.put(serializedName);
            buf.putInt(sizeOfDate);
            buf.put(serializedDate);


            return buf.array();

        } catch (Exception e) {
            throw new SerializationException("Error when serializing Supplier to byte[]");
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}