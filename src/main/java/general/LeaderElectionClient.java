package general;

import dcf.Client;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import org.graalvm.polyglot.*;

public class LeaderElectionClient extends Client {

    public LeaderElectionClient(String clientId, String jsCode, String jsEvaluation, String kafkaServer, String topic) {
        super(clientId, jsCode, jsEvaluation, kafkaServer, topic);
    }

    public void consumeMessage(){

        org.graalvm.polyglot.Context jsContext = Context.create("js");

        try {
            while (!this.getConsensusAchieved()) {
                ConsumerRecords<String, String> records = this.getKafkaConsumer().poll(10);

                for (ConsumerRecord<String, String> record : records) {
                    this.setJsCode(this.getJsCode() + record.value());
                    Value result = jsContext.eval("js",this.getJsCode() + this.getJsEvaluation());

                    Boolean consensusResult = result.getMember("consensus").asBoolean();
                    Value agreedValue = result.getMember("value");

                    if (consensusResult){
                        this.setConsensusAchieved(true);
                        System.out.println("Consensus.");
                        System.out.println(agreedValue);
                        Thread.sleep(5000);
                    }
                    else{
                        System.out.println(false);
                    }
                }
            }

        } catch(Exception exception) {
            System.out.println("Exception occurred while reading messages "+ exception);
            exception.printStackTrace(System.out);
        }finally {
            this.getKafkaConsumer().close();
        }
    }

    public static void electLeader(String clientId, int instanceCount, String kafkaServer){
        final LeaderElectionClient clientInstance = new LeaderElectionClient(clientId, "var clientRanks = [];" +
                "result = {consensus:false, value:\"null\"};",
                "if(Object.keys(clientRanks).length==" + instanceCount + "){" +
                        "var leader = null;"+
                        "var maxRank = 0;"+
                        "for (var i = 0; i < clientRanks.length; i++) {"+
                        "if(clientRanks[i].rank > maxRank){"+
                        "result.consensus=true;" +
                        "result.value = clientRanks[i].client;" +
                        "maxRank = clientRanks[i].rank;" +
                        " }" +
                        "}" +
                        "}" +
                        "result;",
                kafkaServer, "electLeader");

        Runnable consuming = new Runnable() {
            @Override
            public void run() {
                clientInstance.consumeMessage();
            }
        };
        new Thread(consuming).start();

        int clientRank = (int)(1 + Math.random()*100);
        clientInstance.produceMessages("clientRanks.push({client:\""+ clientInstance.getClientId() + "\",rank:" +
                clientRank +"});");
        System.out.println(clientRank);
    }

    public static void main(String args[]){
        LeaderElectionClient.electLeader(args[0], Integer.parseInt(args[1]),"localhost:9092");
    }
}
