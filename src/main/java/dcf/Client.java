package dcf;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public abstract class Client {
    private String clientId;
    private Boolean consensusAchieved = false;
    private String jsCode;
    private String evaluation;
    private KafkaConsumer kafkaConsumer;
    private KafkaProducer kafkaProducer;
    private String topic;

    public Client(String clientId, String jsCode, String evaluation, String kafkaServer, String topic){
        this.clientId = clientId;
        this.jsCode = jsCode;
        this.evaluation = evaluation;
        this.topic = topic;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(kafkaServer, topic, clientId);
        this.kafkaProducer = ProducerGenerator.generateProducer(kafkaServer);
    }

    public void produceMessages(String message) {
        this.kafkaProducer.send(new ProducerRecord<String, String>(this.topic, message));
    }

    public abstract void consumeMessage();

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public Boolean getConsensusAchieved() {
        return consensusAchieved;
    }

    public void setConsensusAchieved(Boolean consensusAchieved) {
        this.consensusAchieved = consensusAchieved;
    }

    public String getJsCode() {
        return jsCode;
    }

    public void setJsCode(String jsCode) {
        this.jsCode = jsCode;
    }

    public String getEvaluation() {
        return evaluation;
    }

    public void setEvaluation(String evaluation) {
        this.evaluation = evaluation;
    }

    public KafkaConsumer getKafkaConsumer() {
        return kafkaConsumer;
    }

    public void setKafkaConsumer(KafkaConsumer kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public KafkaProducer getKafkaProducer() {
        return kafkaProducer;
    }

    public void setKafkaProducer(KafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }
}
