import com.railroad.protocols.emp.decoders.Emp2003Decoder;
import com.railroad.protocols.emp.decoders.Emp2032Decoder;
import com.railroad.protocols.emp.decoders.Emp2080Decoder;
import com.railroad.protocols.emp.decoders.EmpDecoder;
import com.railroad.protocols.emp.models.Emp2003;
import com.railroad.protocols.emp.models.Emp2032;
import com.railroad.protocols.emp.models.Emp2080;
import com.railroad.protocols.emp.utils.ByteArrayUtils;
import com.railroad.protocols.emp.utils.EmpPayload;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class MessageDispenser {
    public static void main(String[] args) {
        int count = 0;
        Producer<Integer, String> producer = new KafkaProducer<>(producerProps());
        try (BufferedReader br = new BufferedReader(new FileReader("src/main/resources/sample.txt"))) {
            String line = br.readLine();
            while (line != null) {
                count++;
                byte[] empMessageBytes = Hex.decodeHex(line.toCharArray());
                String message = getParsedMessage(empMessageBytes);
                if(null != message) {
                    String topic = "Emp" + getMessageTypeId(empMessageBytes);
                    producer.send(new ProducerRecord<>(topic, count, message));
                }
                line = br.readLine();
                System.out.println("==========Message Number: " + count + " sent===========");
            }
        } catch (IOException | DecoderException e) {
            e.printStackTrace();
        }
        producer.close();
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("request.required.acks", "1");
        return props;
    }

    //change the
    private static String getParsedMessage(byte[] empMessageBytes) {
        int messageTypeId = getMessageTypeId(empMessageBytes);
        EmpPayload emp = new EmpPayload(empMessageBytes);
        if(messageTypeId == 2003) {
            EmpDecoder<Emp2003> bodyDecoder = new Emp2003Decoder(emp);
            bodyDecoder.decodeMessage();
            Emp2003 body = bodyDecoder.getBodyObj();
            return body.toString();
        }
        if(messageTypeId == 2032){
            EmpDecoder<Emp2032> bodyDecoder = new Emp2032Decoder(emp);
            bodyDecoder.decodeMessage();
            Emp2032 body = bodyDecoder.getBodyObj();
            return body.toString();
        }
        if(messageTypeId == 2080){
            EmpDecoder<Emp2080> bodyDecoder = new Emp2080Decoder(emp);
            bodyDecoder.decodeMessage();
            Emp2080 body = bodyDecoder.getBodyObj();
            return body.toString();
        }
        return null;
    }

    private static int getMessageTypeId(byte[] b){
        return (int) ByteArrayUtils.byteArrayToLong(b, 1, 3);
    }


}

