# Instrucciones para desplegar Kafka y usar productores/consumidores

## 1. Iniciar el broker Kafka
1. Abre una terminal.
2. Ve al directorio donde está Kafka:
   cd /ruta/a/Kafka
3. Ejecuta el broker con tu configuración:
   bin/kafka-server-start.sh config/server.properties

   Nota: Asegúrate de que en `server.properties` las líneas
   listeners y advertised.listeners usan tu IP:
   listeners=PLAINTEXT://172.27.245.8:9092,CONTROLLER://:9093
   advertised.listeners=PLAINTEXT://172.27.245.8:9092,CONTROLLER://localhost:9093

## 2. Crear un topic
1. Abre otra terminal.
2. Crea el topic SD con:
   bin/kafka-topics.sh --create --topic SD \
   --bootstrap-server 172.27.245.8:9092 \
   --partitions 1 --replication-factor 1
3. Verifica que se creó:
   bin/kafka-topics.sh --list --bootstrap-server 172.27.245.8:9092

## 3. Abrir un productor
1. En otra terminal:
   bin/kafka-console-producer.sh --topic SD --bootstrap-server 172.27.245.8:9092
2. Escribe mensajes y pulsa Enter para enviarlos. Ejemplo:
   Hola Kafka!
   Mensaje de prueba 1
   Mensaje de prueba 2

## 4. Abrir un consumidor
1. En otra terminal:
   bin/kafka-console-consumer.sh --topic SD --bootstrap-server 172.27.245.8:9092 --from-beginning
2. Verás los mensajes que envió el productor en tiempo real.

## 5. Detener Kafka
1. Para detener el broker:
   Ctrl + C en la terminal donde se ejecuta `kafka-server-start.sh`
2. Para detener productor o consumidor:
   Ctrl + C en la terminal correspondiente

