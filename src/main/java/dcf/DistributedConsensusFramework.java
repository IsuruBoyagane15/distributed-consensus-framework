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
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(distributedNode.getKafkaServerAddress(),
                distributedNode.getKafkaTopic(), distributedNode.getNodeId());
        this.kafkaProducer = ProducerGenerator.generateProducer(distributedNode.getKafkaServerAddress());
    }

    public void start(){

        final String initialJsCode = this.distributedNode.getRuntimeJsCode();

        writeACommand("IN,"+ distributedNode.getNodeId());

        Runnable consuming = new Runnable() {
            public void run() {
                boolean consensusAchieved = false;
                boolean correctRoundIdentified = false;
                ArrayList<String> participantIds = new ArrayList<String>();
                try {
                    while (!consensusAchieved) {
                        ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
                        for (ConsumerRecord<String, String> record : records) {
                            if (record.value().startsWith("IN,")) {
                                String[] inCommand = record.value().split(",");
                                participantIds.add(inCommand[1]);
                                if (inCommand[1].equals(distributedNode.getNodeId())) {
                                    correctRoundIdentified = true;
                                }
                            }
                            else if (record.value().startsWith("RESET,")){
                                String[] resetCommands = record.value().split(",");
                                participantIds.remove(resetCommands[1]);
                                if (participantIds.size() == 0){
                                    distributedNode.setRuntimeJsCode(initialJsCode);
                                }
                            }
                            else{
                                Value result = evaluateJsCode(record.value());
                                consensusAchieved = distributedNode.onReceiving(result);
                                if (consensusAchieved){
                                    if (correctRoundIdentified){
                                        distributedNode.commitAgreedValue(result);
                                        writeACommand("RESET,"+ distributedNode.getNodeId());
                                    }
                                }
                            }
                        }
                    }
                } catch(Exception exception) {
                    LOGGER.error("Exception occurred :", exception);
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
        distributedNode.setRuntimeJsCode(distributedNode.getRuntimeJsCode() + command);
        return jsContext.eval("js",distributedNode.getRuntimeJsCode()+
                distributedNode.getEvaluationJsCode());
    }
}
