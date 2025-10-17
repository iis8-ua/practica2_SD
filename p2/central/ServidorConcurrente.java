package p2.evcharging.central;

import common.util.KafkaProducerHelper;
import common.util.KafkaConsumerHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class ServidorConcurrente {

    private final KafkaProducerHelper producer;
    private final KafkaConsumerHelper consumer;

    public ServidorConcurrente(KafkaProducerHelper producer, KafkaConsumerHelper consumer) {
        this.producer = producer;
        this.consumer = consumer;
    }

    public void iniciar() {
        System.out.println("🛰️ ServidorConcurrente escuchando mensajes Kafka...");
        consumer.pollLoop(record -> {
            Thread hilo = new Thread(new HiloServidor(record, producer));
            hilo.start();
        });
    }
}
