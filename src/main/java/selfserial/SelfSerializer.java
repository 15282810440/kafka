package selfserial;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import vo.DemoUser;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 自定义序列化器  将需要序列化得对象转换成byte数组
 */
public class SelfSerializer implements Serializer<DemoUser> {
    @Override
    public void configure(Map configs, boolean isKey) {
        //not do thing
    }

    @Override
    public byte[] serialize(String s, DemoUser demoUser) {
        try{
            if(demoUser == null){
                return null;
            }
            byte [] nameByte;
            int nameSize = 0;
            if(demoUser.getName() == null){
                nameByte = new byte[0];
                nameSize = 0;
            }else{
                nameByte  = demoUser.getName().getBytes("UTF-8");
                nameSize = nameByte.length;
            }
            ByteBuffer byteBuffer  = ByteBuffer.allocate(4+4+nameSize);
            byteBuffer.putInt(demoUser.getId());
            byteBuffer.putInt(nameSize);
            byteBuffer.put(nameByte);
            return byteBuffer.array();
        }catch (Exception e){
            throw new SerializationException("Error serialize DemoUser:"+e);
        }
    }

    @Override
    public void close() {
        //not do thing
    }
}
