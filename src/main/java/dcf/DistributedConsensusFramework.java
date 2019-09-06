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
    private String nodeId, runtimeJsCode, evaluationJsCode, kafkaTopic;
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaProducer<String, String> kafkaProducer;
    private org.graalvm.polyglot.Context jsContext;
    private ConsensusApplication app;

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedConsensusFramework.class);

    public DistributedConsensusFramework(String nodeId, String runtimeJsCode, final String evaluationJsCode,
                                         String kafkaServerAddress, String kafkaTopic, final ConsensusApplication app){
        this.nodeId = nodeId;
        this.runtimeJsCode = runtimeJsCode;
        this.evaluationJsCode = evaluationJsCode;
        this.kafkaTopic = kafkaTopic;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(kafkaServerAddress, kafkaTopic, nodeId);
        this.kafkaProducer = ProducerGenerator.generateProducer(kafkaServerAddress);
        this.jsContext = Context.create("js");
        this.app = app;

        Runnable consuming = new Runnable() {
            public void run() {
            Boolean consensusAchieved = false;
            try {
                while (!consensusAchieved) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(10);

                    for (ConsumerRecord<String, String> record : records) {
                        if (record.value().equals("RESET")){
                            setRuntimeJsCode("var clientRanks = [];" + "result = {consensus:false, value:null};");
                        }
                        else {
                            Value result = evaluateJsCode(record.value());
                            consensusAchieved = app.onReceiving(result);
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
        this.kafkaProducer.send(new ProducerRecord<String, String>(this.kafkaTopic, command));
    }

    public Value evaluateJsCode(String command){
        this.runtimeJsCode = this.runtimeJsCode +  command;
        String jsCodeToEvaluate = this.runtimeJsCode + this.evaluationJsCode;
        return this.jsContext.eval("js",jsCodeToEvaluate);
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getRuntimeJsCode() {
        return runtimeJsCode;
    }

    public void setRuntimeJsCode(String runtimeJsCode) {
        this.runtimeJsCode = runtimeJsCode;
    }

    public String getEvaluationJsCode() {
        return evaluationJsCode;
    }

    public void setEvaluationJsCode(String evaluationJsCode) {
        this.evaluationJsCode = evaluationJsCode;
    }

    public KafkaConsumer<String, String> getKafkaConsumer() {
        return kafkaConsumer;
    }

}
