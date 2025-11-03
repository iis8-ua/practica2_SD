package p2.driver;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;

public class EV_Driver {
	private String driverId;
    private String dirKafka;
    private KafkaProducer<String, String> productor;
    private KafkaConsumer<String, String> consumidor;
    private boolean ejecucion;
    private Scanner scanner;
    private String cp;
    private String sesion;
    
    public static void main(String[] args) {
		//para que no aparezcan los mensajes de kafka en la central 
    	System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "WARN");
    	System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", "WARN");
    	System.setProperty("org.slf4j.simpleLogger.log.kafka", "WARN");
    	System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka.clients", "WARN");
    	System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka.common", "WARN");
    	System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka.clients.network", "ERROR");
    	System.setProperty("org.slf4j.simpleLogger.log.org.slf4j", "WARN");
    	java.util.logging.Logger.getLogger("org.apache.kafka").setLevel(java.util.logging.Level.SEVERE);
    	
    	if (args.length < 2) {
    		System.out.println("Uso: java EV_Driver <dirKafka> <driverId> [archivoServicios]");
            return;
        }
    	
    	String dirKafka=args[0];
    	String driverId=args[1];
    	String archivo;
    	
    	if(args.length>2) {
    		archivo=args[2];
    	}
    	else {
    		archivo=null;
    	}
    	
    	EV_Driver driver=new EV_Driver();
    	driver.iniciar(dirKafka, driverId, archivo);
    }

	public void iniciar(String dirKafka, String driverId, String archivo) {
		try {
			this.dirKafka=dirKafka;
			this.driverId=driverId;
			this.scanner=new Scanner(System.in);
			this.cp=null;
			this.sesion=null;
			
			configurarKafka();
			this.ejecucion=true;
			System.out.println("Driver " + driverId + " iniciado");
			
			//se usa el hilo para que no se bloquee el programa y haya ejecucion paralela y no se quede esperando los mensajes sin poder ejecutar el menu
			Thread hilo= new Thread(this::procesarMensajes);
			hilo.start();
			
			if(archivo!=null) {
				procesarArchivo(archivo);
			}
			else {
				menu();
			}
		}
		catch(Exception e) {
			System.err.println("Error iniciando driver: " + e.getMessage());
		}
		finally {
			detener();
		}
	}

	private void configurarKafka() {
		try {
			 Properties propiedadesProductor = new Properties();
			 propiedadesProductor.put("bootstrap.servers", dirKafka);
			 propiedadesProductor.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			 propiedadesProductor.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			 this.productor = new KafkaProducer<>(propiedadesProductor);
			 
			 Properties propiedadesConsumidor = new Properties();
			 propiedadesConsumidor.put("bootstrap.servers", dirKafka);
			 propiedadesConsumidor.put("group.id", "driver-" + driverId);
			 propiedadesConsumidor.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			 propiedadesConsumidor.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			 propiedadesConsumidor.put("auto.offset.reset", "earliest");
			 this.consumidor = new KafkaConsumer<>(propiedadesConsumidor);
			 
			 consumidor.subscribe(Arrays.asList(
			            "driver-autorizacion-" + driverId,
			            "driver-estado-" + driverId,
			            "driver-ticket-" + driverId,
			            "driver-error-" + driverId
			 ));
		}
		catch(Exception e) {
            System.err.println("Error configurando Kafka: " + e.getMessage());
		}
	 }
	
	
	 private void procesarMensajes() {
		 while(ejecucion) {
			 try {
				 ConsumerRecords<String, String> records = consumidor.poll(Duration.ofMillis(1000));
				 records.forEach(record -> {procesarMensaje(record.topic(), record.key(), record.value());});
			 }
			 catch(Exception e) {
				 if(ejecucion) {
					 System.err.println("Error procesando mensajes: " + e.getMessage());
				 }
			 }
		 }
	 }
	 
	private void procesarMensaje(String tema, String key, String mensaje) {
		if(tema.equals("driver-autorizacion-" + driverId)){
			procesarAutorizacion(mensaje);
		}
		else if(tema.equals("driver-estado-" + driverId)){
			procesarEstado(mensaje);
		}
		else if(tema.equals("driver-ticket-" + driverId)){
			procesarTicket(mensaje);
		}
		else {
			System.out.println("Tema no reconocido: " + tema);
		}
	}
	
	private void procesarAutorizacion(String mensaje) {
		String[] partes= mensaje.split("\\|");
		String tipo=partes[0];
		String cpId=partes[1];
		
		if("Autorizado".equals(tipo)) {
			this.sesion=partes[2];
			this.cp=cpId;
			System.out.println("Autorizado - Conecte su vehículo a " + cpId);
            System.out.println("Sesión: " + sesion);
		}
		else if("Denegado".equals(tipo)) {
			this.cp=null;
			this.sesion=null;
			System.out.println("Denegado, CP " + cpId +" no disponible");
		}
	}
	
	private void procesarEstado(String mensaje) {
		String[] partes= mensaje.split("\\|");
		String cpId=partes[1];
		String consumo=partes[2];
		String importe=partes[3];
		String estado=partes[4];
		
		System.out.printf("%s | Consumo: %s kW | Importe: %s € | Estado: %s%n", cpId, consumo, importe, estado);
	}
	
	
	private void procesarTicket(String mensaje) {
		String[] partes= mensaje.split("\\|");
		String cpId=partes[1];
		String consumo=partes[2];
		String importe=partes[3];

		System.out.println("\n=== TICKET ===");
        System.out.println("CP: " + cpId);
        System.out.println("Conductor: " + driverId);
        System.out.printf("Consumo total: %s kW%n", consumo);
        System.out.printf("Importe total: %s €%n", importe);
        System.out.println("=====================\n");
        
        this.cp=null;
        this.sesion=null;
	}
	
	private void procesarArchivo(String archivo) {
		try {
			File arch= new File(archivo);
			if(!arch.exists()) {
				System.err.println("Archivo no encontrado: " + archivo);
				return;
			}
			
			BufferedReader leer= new BufferedReader(new FileReader(archivo));
			String cpId;
			int contador=0;
			
			while((cpId=leer.readLine()) !=null && ejecucion) {
				cpId=cpId.trim();
				if(!cpId.isEmpty()) {
					contador++;
					System.out.println("\n=== Servicio " + contador + " ===");
					System.out.println("Solicitando servicio en CP: " + cpId);
					solicitarServicio(cpId);
					esperarFinServicio();
					
					if(ejecucion) {
						Thread.sleep(4000);
					}
				}
			}
			leer.close();
			
			if(contador==0) {
				System.out.println("No hay servicios en el archivo o los CPs no estan disponibles");
			}
			else {
				System.out.println("Se han completado: " + contador + " servicios");
			}
		}
		catch(Exception e) {
			System.err.println("Error procesando archivo: " + e.getMessage());
		}
	}
	
	private void esperarFinServicio() {
		//Se tiene que esperar que el servicio actual se termine de ejecutar, como maximo 2 minutos por servicio
		int timeout=120;
		while(cp!=null && timeout>0 && ejecucion) {
			try {
				Thread.sleep(1000);
				timeout--;
				if(timeout %10==0) {
					System.out.println("Esperando al servicio anterior... " + timeout + "s restantes");
				}
			}
			catch(InterruptedException e) {
				break;
			}
			
		}
		
		if(timeout<0 && cp!=null) {
			cp=null;
			sesion=null;
			System.err.println("Timeout expirado");
		}
	}
	
	private void menu() {
		while(ejecucion) {
			System.out.println("\n--- MENÚ DRIVER " + driverId + " ---");
            System.out.println("1. Solicitar servicio en CP");
            System.out.println("2. Ver estado actual");
            System.out.println("3. Salir");
            System.out.print("Seleccione opción: ");
            
            try {
            	int opcion=scanner.nextInt();
            	scanner.nextLine();
            	
            	switch(opcion) {
            		case 1:
            			System.out.println("Ingrese ID del CP: ");
            			String cpId=scanner.nextLine().trim();
            			if(!cpId.isEmpty()) {
            				solicitarServicio(cpId);
            			}
            			break;
            		case 2:
            			mostrarEstado();
            			break;
            		case 3:
            			ejecucion=false;
            			break;
            		default:
            			System.out.println("Opcion invalida");
            	}
            }
            catch(Exception e) {
            	System.err.println("Error en menú: " + e.getMessage());
            	scanner.nextLine();
            }
		}
		
	}
	
	private void mostrarEstado() {
		System.out.println("\n=== ESTADO ACTUAL DRIVER " + driverId + " ===");
		if(cp!=null) {
			System.out.println(cp);
		}
		else {
			System.out.println("Ninguno");
		}
		System.out.println("=================================");
	}
	
	private void solicitarServicio(String cpId) {
		try {
			String mensaje= String.format("Solicitud_Servicio|%s|%s", driverId, cpId);
			ProducerRecord<String, String> record = new ProducerRecord<>("driver-solicitud", driverId, mensaje);
			productor.send(record);
			System.out.println("Solicitud enviada a la central para CP: " + cpId);
		}
		catch(Exception e) {
			System.err.println("Error solicitando servicio: " + e.getMessage());
		}
	}

	private void detener() {
		ejecucion=false;
		try {
			if(productor !=null) {
				productor.close();
			}
			if(consumidor !=null) {
				consumidor.close();
			}
			if(scanner!=null) {
				scanner.close();
			}
			System.out.println("Driver detenido");
		}
		catch(Exception e) {
			System.err.println("Error deteniendo driver: " + e.getMessage());
		}
	}
    
}
