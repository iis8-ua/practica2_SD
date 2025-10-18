package p2.central;

import common.util.KafkaProducerHelper;
import common.util.KafkaConsumerHelper;

import java.util.Arrays;

public class EV_Central {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: java p2.evcharging.central.EV_Central <bootstrapServers>");
            System.out.println("Ejemplo: java p2.evcharging.central.EV_Central localhost:9092");
            System.exit(1);
        }

        String bootstrapServers = args[0];

        System.out.println("EV_Central conectado a Kafka en " + bootstrapServers);

        KafkaProducerHelper producer = new KafkaProducerHelper(bootstrapServers);
        KafkaConsumerHelper consumer = new KafkaConsumerHelper(
                bootstrapServers,
                "central-group",
                Arrays.asList("cp-to-central")
        );

        ServidorConcurrente servidor = new ServidorConcurrente(producer, consumer);
        servidor.iniciar();
    }
}
