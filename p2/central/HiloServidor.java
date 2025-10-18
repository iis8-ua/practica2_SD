package p2.central;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import common.util.KafkaProducerHelper;

public class HiloServidor extends Thread {

    private final ConsumerRecord<String, String> record;
    private final GestionCentral gestion;
    private final KafkaProducerHelper producer;

    public HiloServidor(ConsumerRecord<String, String> record, KafkaProducerHelper producer) {
        this.record = record;
        this.gestion = new GestionCentral();
        this.producer = producer;
    }

    @Override
    public void run() {
        try {
            String mensaje = record.value();
            System.out.println("Mensaje recibido: " + mensaje);

            // Mantiene el mismo formato con separador |
            String[] partes = mensaje.split("\\|");
            if (partes.length < 1) {
                System.out.println("Mensaje mal formado");
                return;
            }

            String respuesta = gestion.procesarMensaje(partes);

            // Enviar respuesta a topic de salida (central-to-cp)
            String cpId = partes.length > 1 ? partes[1] : "unknown";
            producer.send("central-to-cp", cpId, respuesta);

            System.out.println("Respuesta enviada a topic central-to-cp: " + respuesta);

        } catch (Exception e) {
            System.out.println("Error en HiloServidorKafka: " + e);
        }
    }
}
