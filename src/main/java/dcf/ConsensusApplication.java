package dcf;

import org.graalvm.polyglot.Value;

public abstract class ConsensusApplication {
    private String nodeId, runtimeJsCode, evaluationJsCode, kafkaTopic;
    private Boolean consensusAchieved = false;

    public ConsensusApplication(String nodeId, String runtimeJsCode, String evaluationJsCode, String kafkaServerAddress, String kafkaTopic){
        this.nodeId = nodeId;
        this.runtimeJsCode = runtimeJsCode;
        this.evaluationJsCode = evaluationJsCode;
        this.kafkaTopic = kafkaTopic;
    }

    public abstract Boolean onReceiving(Value evaluationOutput);

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public Boolean getConsensusAchieved() {
        return consensusAchieved;
    }

    public void setConsensusAchieved(Boolean consensusAchieved) {
        this.consensusAchieved = consensusAchieved;
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

}
