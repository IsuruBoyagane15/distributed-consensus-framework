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

public class DistributedConsensusFramework {
//    private String nodeId, runtimeJsCode, evaluationJsCode, kafkaTopic;
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
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(distributedNode.getKafkaServerAddress(), distributedNode.getKafkaTopic(), distributedNode.getNodeId());
        this.kafkaProducer = ProducerGenerator.generateProducer(distributedNode.getKafkaServerAddress());

        Runnable consuming = new Runnable() {
            public void run() {
                Boolean consensusAchieved = false;
                try {
                    while (!consensusAchieved) {
                        ConsumerRecords<String, String> records = kafkaConsumer.poll(10);
                        for (ConsumerRecord<String, String> record : records) {
                            if (record.value().equals("RESET")){
                                distributedNode.setRuntimeJsCode("var clientRanks = [];" + "result = {consensus:false, value:null};");
                            }
                            else {
                                Value result = evaluateJsCode(record.value());
                                consensusAchieved = distributedNode.onReceiving(result);
                            }
                        }
                    }
                } catch(Exception exception) {
                    LOGGER.error("Exception occurred while processing command", exception);
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
