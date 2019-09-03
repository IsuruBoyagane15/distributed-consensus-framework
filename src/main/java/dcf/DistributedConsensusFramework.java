package dcf;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Value;

public class DistributedConsensusFramework {
    private String nodeId, runtimeJsCode, evaluationJsCode, kafkaTopic;
    private KafkaConsumer<String, String> kafkaConsumer;
    private KafkaProducer<String, String> kafkaProducer;
    private org.graalvm.polyglot.Context jsContext;

    public DistributedConsensusFramework(String nodeId, String runtimeJsCode, String evaluationJsCode,
                                         String kafkaServerAddress, String kafkaTopic){
        this.nodeId = nodeId;
        this.runtimeJsCode = runtimeJsCode;
        this.evaluationJsCode = evaluationJsCode;
        this.kafkaTopic = kafkaTopic;
        this.kafkaConsumer = ConsumerGenerator.generateConsumer(kafkaServerAddress, kafkaTopic, nodeId);
        this.kafkaProducer = ProducerGenerator.generateProducer(kafkaServerAddress);
        this.jsContext = Context.create("js");
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
