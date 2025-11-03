package p2.central;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.time.Duration;


public class HiloServidor extends Thread {
	private KafkaProducer<String, String> productor;
    private KafkaConsumer<String, String> consumidor;
    private boolean ejecucion;
		
	public HiloServidor(KafkaProducer<String, String> productor, KafkaConsumer<String, String> consumidor) {
		 this.productor=productor;
		 this.consumidor=consumidor;
		 this.ejecucion=false;
	}

	@Override
	public void run() {
		this.ejecucion=true;
		procesarMensajesKafka();
	}

	 private void procesarMensajesKafka() {
		 while(ejecucion) {
			 try {
				 //Libreria para que el consumidor reciba los mensajes desde kafka
				 ConsumerRecords<String, String> records = consumidor.poll(Duration.ofMillis(100));
				 //cada mensaje que llega se procesa en un hilo separado al resto
				 records.forEach(record -> {
					 new Thread(() -> procesarMensaje(record.topic(), record.key(), record.value())).start();
				 });
			 }
			 catch(Exception e) {
				 if(ejecucion) {
					 System.err.println("Error procesando los mensajes de Kafka desde los hilos: " + e.getMessage());
				 }
			 }
		 }
	 }
	
	private void procesarMensaje(String tema, String cpId, String mensaje) {
		//System.out.println("Se ha recibido - Tema: " + tema + ", CP: " + cpId + ", Mensaje: " + mensaje);
		
		try {
			switch (tema) {
	            case "cp-registro":
	                procesarRegistro(cpId, mensaje);
	                break;
	            case "cp-estado":
	                procesarActualizacionEstado(cpId, mensaje);
	                break;
	            case "cp-autorizacion":
	                procesarAutorizacion(cpId, mensaje);
	                break;
	            case "actualizacion-recarga":
	                procesarActualizacionRecarga(cpId, mensaje);
	                break;
	            case "fallo-cp":
	                procesarAveria(cpId, mensaje);
	                break;
	            case "recuperacion-cp":
	                procesarRecuperacion(cpId, mensaje);
	                break;
	            case "monitor-registro":
	            	procesarRegistroMonitor(cpId, mensaje);
	            	break;
	            case "ticket":
	            	procesarTicket(cpId, mensaje);
	            case "driver-solicitud":
	            	procesarSolicitudDriver(cpId, mensaje);
	            	break;
	            default:
	                System.out.println("Tema no reconocido: " + tema);
			}
		}
		catch(Exception e) {
			System.err.println("Error procesando el mensaje: " + e.getMessage());
		}
	}

	private void procesarSolicitudDriver(String driverId, String mensaje) {
		 System.out.println("Solicitud recibida de driver: " + driverId);
		 
		 String[] partes= mensaje.split("\\|");
		 if(partes.length<4) {
			 
		 }
	}
	
	private void enviarParar(String cpId) {
		String mensaje="Parar";
		ProducerRecord<String, String> record = new ProducerRecord<>("central-to-cp", cpId, mensaje);
		productor.send(record);
		System.out.println("Comando parar enviado a CP: " + cpId);
	}
	
	private void enviarReanudar(String cpId) {
		String mensaje="Reanudar";
		ProducerRecord<String, String> record = new ProducerRecord<>("central-to-cp", cpId, mensaje);
		productor.send(record);
		System.out.println("Comando reanudar enviado a CP: " + cpId);
	}

	private void procesarRegistroMonitor(String cpId, String mensaje) {
		System.out.println("Monitor registrado para CP: " + cpId);
	    
	    String confirmacion = "Monitor_Registro_OK|" + cpId;
	    productor.send(new ProducerRecord<>("central-to-monitor", cpId, confirmacion));
	}

	private void procesarRecuperacion(String cpId, String mensaje) {
		if(mensaje.startsWith("Recuperacion_Reporte")) {
			System.out.println("Reporte del monitor, recuperacion en CP: " + cpId);
		}
		else {
			System.out.println("Recuperacion en CP: " + cpId);
		}
		
		String confirmacion = "Recuperacion_ACK|" + cpId;
        productor.send(new ProducerRecord<>("central-to-cp", cpId, confirmacion));
        
        String confirmacion2= "Recuperacion|" + cpId;
        productor.send(new ProducerRecord<>("sistema-eventos", cpId, confirmacion2));
	}

	private void procesarAveria(String cpId, String mensaje) {
		if(mensaje.startsWith("Averia_Reporte")) {
			System.out.println("Reporte del monitor, averia en CP: " + cpId);
		}
		else {
			System.out.println("Averia en CP: " + cpId);
		}
		
		String confirmacion = "Averia_ACK|" + cpId;
        productor.send(new ProducerRecord<>("central-to-cp", cpId, confirmacion));
        
        String confirmacion2= "Averia|" + cpId;
        productor.send(new ProducerRecord<>("sistema-eventos", cpId, confirmacion2));

	}

	private void procesarActualizacionRecarga(String cpId, String mensaje) {
		String[] partes = mensaje.split("\\|");
        String consumo = partes[3];
        String importe = partes[2];
        
        System.out.printf("CP: %s | Consumo: %s kW | Importe: %s €%n", cpId, consumo, importe);
        
        String confirmacion = "Consumo_OK|" + cpId;
        productor.send(new ProducerRecord<>("central-to-cp", cpId, confirmacion));
	}

	private void procesarAutorizacion(String cpId, String mensaje) {
		System.out.println("Procesando autorización CP " + cpId + ": " + mensaje);
		
		if(mensaje.contains("Aceptada")) {
			String confirmacion = "Autorización_OK_ACK|" + cpId;
	        productor.send(new ProducerRecord<>("central-to-cp", cpId, confirmacion));
		}
		else {
			String confirmacion = "Autorización_DEN_ACK|" + cpId;
	        productor.send(new ProducerRecord<>("central-to-cp", cpId, confirmacion));
		}
	}

	private void procesarActualizacionEstado(String cpId, String mensaje) {
		String[] partes = mensaje.split("\\|");
        String estado = partes[2];
        String funciona = partes[3];

        System.out.println("Estado actualizado CP: " + cpId + ": " + estado + " - Funciona: " + funciona);
        
        String confirmacion = "Actualizacion_OK|" + cpId;
        productor.send(new ProducerRecord<>("central-to-cp", cpId, confirmacion));
	}

	private void procesarRegistro(String cpId, String mensaje) {
		String[] partes = mensaje.split("\\|");
        String ubicacion = partes[2];
        String precio = partes[3];

        System.out.println("CP registrado: " + cpId + " en " + ubicacion + " - Precio: " + precio);
        
        String confirmacion = "Registro_OK|" + cpId;
        productor.send(new ProducerRecord<>("central-to-cp", cpId, confirmacion));
		
	}
	
	private void procesarTicket(String cpId, String mensaje) {
		String[] partes = mensaje.split("\\|");
	    String conductorId = partes[1];
	    String consumo = partes[3];
	    String importe = partes[2];
	    
	    System.out.printf("Ticket enviado a %s - %s: %s kW (%s €)%n", conductorId, cpId, consumo, importe);
	    String confirmacion = "Ticket_ACK|" + cpId;
        productor.send(new ProducerRecord<>("central-to-cp", cpId, confirmacion));
	}

	public void detener() {
		ejecucion=false;
		this.interrupt();
		 System.out.println("Hilo servidor detenido");
	}
}
