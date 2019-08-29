package dcf;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public abstract class ConsensusApplication {
    private String nodeId;
    private Boolean consensusAchieved = false;
    private String initialJsCode;
    private String evaluationJsCode;
    private KafkaConsumer kafkaConsumer;
    private KafkaProducer kafkaProducer;
    private String kafkaTopic;

    public ConsensusApplication(String clientId, String initialJsCode, String evaluationJsCode, String kafkaServerAddress, String kafkaTopic){
        this.nodeId = clientId;
        this.initialJsCode = initialJsCode;
        this.evaluationJsCode = evaluationJsCode;
        this.kafkaTopic = kafkaTopic;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(kafkaServerAddress, kafkaTopic, clientId);
        this.kafkaProducer = ProducerGenerator.generateProducer(kafkaServerAddress);
    }

    public void writeACommand(String message) {
        this.kafkaProducer.send(new ProducerRecord<String, String>(this.kafkaTopic, message));
    }

    public abstract void processACommand();

    public String getNodeId() {
        return nodeId;
    }

    public Boolean getConsensusAchieved() {
        return consensusAchieved;
    }

    public void setConsensusAchieved(Boolean consensusAchieved) {
        this.consensusAchieved = consensusAchieved;
    }

    public String getInitialJsCode() {
        return initialJsCode;
    }

    public void setInitialJsCode(String initialJsCode) {
        this.initialJsCode = initialJsCode;
    }

    public String getEvaluationJsCode() {
        return evaluationJsCode;
    }

    public void setEvaluationJsCode(String evaluationJsCode) {
        this.evaluationJsCode = evaluationJsCode;
    }

    public KafkaConsumer getKafkaConsumer() {
        return kafkaConsumer;
    }

}
