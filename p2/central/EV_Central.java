package p2.central;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;

//El StringSerializer y StringDeserializer lo que hacen es la conversion de string a bytes y viceversa
//ya que kafka trabaja con bytes


public class EV_Central {

    public static void main(String[] args) {
        if (args.length < 1) {
        	System.out.println("Uso: java p2.central.EV_Central <host:puerto>");
            //System.out.println("Ejemplo: java p2.central.EV_Central localhost:9092");
            System.exit(1);
        }

        String dirKafka = args[0];
        System.out.println("Central iniciando...");
        System.out.println("Conectando a Kafka en: " + dirKafka);
        
        try {
        	 //Configuracion productor con Kafka
            Properties propiedadesProductor = new Properties();
            propiedadesProductor.put("bootstrap.servers", dirKafka);
            propiedadesProductor.put("key.serializer", StringSerializer.class.getName());
            propiedadesProductor.put("value.serializer", StringSerializer.class.getName());
            propiedadesProductor.put("acks", "1");
            KafkaProducer<String, String> productor = new KafkaProducer<>(propiedadesProductor);
            
            //Configuracion propiedades consumidor en Kafka
            Properties propiedadesConsumidor = new Properties();
            propiedadesConsumidor.put("bootstrap.servers", dirKafka);
            propiedadesConsumidor.put("group.id", "central-group");
            propiedadesConsumidor.put("key.deserializer", StringDeserializer.class.getName());
            propiedadesConsumidor.put("value.deserializer", StringDeserializer.class.getName());
            propiedadesConsumidor.put("auto.offset.reset", "earliest");
            KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(propiedadesConsumidor);
            
            ServidorConcurrente servidor = new ServidorConcurrente(productor, consumidor);
            servidor.iniciar();
            System.out.println("EV_Central iniciado correctamente");
            System.out.println("Escuchando mensajes de los CP...");
            
            mantenerEjecucion();
        }
        catch(Exception e) {
        	System.err.println("Error iniciando la Central: " + e.getMessage());
        }
    }

	private static void mantenerEjecucion() {
		try {
			while(true) {
				Thread.sleep(1000);
			}
		}
		catch(Exception e) {
			System.out.println("Central interrumpida");
		}
	}
}
