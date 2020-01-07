package selfserial;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import vo.DemoUser;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 自定义序列化器  将需要序列化得对象转换成byte数组
 */
public class SelfDeSerializer implements Deserializer<DemoUser> {
    @Override
    public DemoUser deserialize(String s, byte[] bytes) {
        try {
            if(bytes == null){
                return null;
            }
            if(bytes.length < 8){
                throw new SerializationException("Error data size.");
            }
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            int id = byteBuffer.getInt();
            int nameSize = byteBuffer.getInt();
            byte [] nameByte = new byte[nameSize];
            byteBuffer.get(nameByte);
            String name = new String(nameByte, "UTF-8");
            DemoUser demoUser = new DemoUser(id, name);
            return demoUser;
        } catch (Exception e) {
            throw new SerializationException("Error Deserializer DemoUser."+e);
        }
    }
}
