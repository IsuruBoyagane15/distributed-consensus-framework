package dcf;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class DistributedConsensusFramework {
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaProducer<String, String> kafkaProducer;
    private org.graalvm.polyglot.Context jsContext;
    private ConsensusApplication distributedNode;

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedConsensusFramework.class);

    public DistributedConsensusFramework(ConsensusApplication distributedNode){
        this.jsContext = Context.create("js");
        this.distributedNode  = distributedNode;
    }

    public void start(){
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(distributedNode.getKafkaServerAddress(),
                distributedNode.getKafkaTopic(), distributedNode.getNodeId());
        this.kafkaProducer = ProducerGenerator.generateProducer(distributedNode.getKafkaServerAddress());

        final String initialJsCode = this.distributedNode.getRuntimeJsCode();

        writeACommand("IN,"+distributedNode.getNodeId());

        Runnable consuming = new Runnable() {
            public void run() {
                Boolean consensusAchieved = false;
                Boolean resetDone = false;
                ArrayList<String> participants = new ArrayList<String>();
                try {
                    while (!resetDone) {
                        ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
                        for (ConsumerRecord<String, String> record : records) {

                            if (record.value().startsWith("RESET,") || record.value().startsWith("IN,")){
                                String[] commands = record.value().split(",");
                                if (commands[0].equals("RESET")){
                                    participants.remove(commands[1]);
                                    if (participants.size() == 0){
                                        distributedNode.setRuntimeJsCode(initialJsCode);
                                        if (consensusAchieved){
                                            resetDone = true;
                                        }
                                    }
                                }
                                if (commands[0].equals("IN")){
                                    participants.add(commands[1]);
                                }
                            }
                            else {
                                Value result = evaluateJsCode(record.value());
                                consensusAchieved = distributedNode.onReceiving(result);
                                if (consensusAchieved){
                                    writeACommand("RESET,"+ distributedNode.getNodeId());
                                }
                            }
                        }
                    }
                } catch(Exception exception) {
                    LOGGER.error("Exception occurred while processing command :", exception);
                }finally {
                    kafkaConsumer.close();
                }
            }
        };
        new Thread(consuming).start();
    }

    public void writeACommand(String command) {
        kafkaProducer.send(new ProducerRecord<String, String>(distributedNode.getKafkaTopic(), command));
    }

    public Value evaluateJsCode(String command){
        distributedNode.setRuntimeJsCode(distributedNode.getRuntimeJsCode() +  command);
        return jsContext.eval("js",distributedNode.getRuntimeJsCode()+ distributedNode.getEvaluationJsCode());
    }
}
