package p2.evcharging.cp;

import p2.evcharging.cp.network.CentralConnector;
import java.io.*;
import java.net.Socket;
import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class EV_CP_M {
	private String cpId;
    private String hostEngine;
    private int puertoEngine;
    private String dirKafka;
    private boolean ejecucion;
    private CentralConnector conector;
    private Scanner scanner;
    
    
    public static void main(String[] args) {
    	//para que no aparezcan los mensajes de kafka en la central 
    	System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
    	System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", "error");
    	System.setProperty("org.slf4j.simpleLogger.log.kafka", "error");
    	System.setProperty("org.slf4j.simpleLogger.log.org.apache.zookeeper", "error");
    	System.setProperty("org.slf4j.simpleLogger.log.org.slf4j", "off");
    	java.util.logging.Logger.getLogger("org.apache.kafka").setLevel(java.util.logging.Level.SEVERE);
    	
        if (args.length < 4) {
        	System.out.println("Uso: java EV_CP_M <host_engine> <puerto_engine> <cp_id> <dirKafka>");
            //System.out.println("Ej: java EV_CP_M localhost 8080 CP001 localhost:9092");
            return;
        }

        String hostEngine = args[0];
        int puertoEngine = Integer.parseInt(args[1]);
        String cpId = args[2];
        String dirKafka = args[3];

        EV_CP_M monitor = new EV_CP_M();
        monitor.iniciar(hostEngine, puertoEngine, cpId, dirKafka);
    }
    
    public void escribirDatos(Socket sock, String datos) {
		try {
			OutputStream aux= sock.getOutputStream();
			DataOutputStream flujo = new DataOutputStream(aux);
			flujo.writeUTF(datos);
		}
		catch (Exception e) {
			System.out.println("Error al escribir datos " + e.toString());
		}
	}
	
	public String leerDatos(Socket sock) {
		String datos = "";
		try {
			InputStream aux= sock.getInputStream();
			DataInputStream flujo = new DataInputStream(aux);
			datos=flujo.readUTF();
		}
		catch(EOFException eof) {
			return null;
		}
		
		catch (IOException e) {
			System.out.println("Error al leer datos " + e.toString());
		}
		return datos;
	}
    
    public void iniciar(String hostEngine, int puertoEngine, String cpId, String dirKafka) {
    	try {
    		this.hostEngine = hostEngine;
            this.puertoEngine = puertoEngine;
            this.cpId = cpId;
            this.dirKafka = dirKafka;
            this.ejecucion = true;
            this.scanner = new Scanner(System.in);
            
            if (!conectarCentral()) {
            	System.err.println("No se ha podido establecer conecion con la central. Saliendo...");
                return;
            }
            
            System.out.println("Monitor iniciado para el CP: " + cpId);
            System.out.println("Conectado al Engine en: " + hostEngine + ":" + puertoEngine);
            System.out.println("Conectado a Central en: " + dirKafka);
            ejecutarBuclePrincipal();
    	}
    	catch(Exception e) {
    		System.err.println("Error en el inicio del monitor: " + e.getMessage());
    	}
    	finally {
    		detener();
    	}
    }
    

	private void detener() {
		ejecucion=false;
		try {
			if(conector !=null) {
				conector.cerrarConexiones();
			}
			
			if(scanner !=null) {
				scanner.close();
			}
			
			System.out.println("Monitor detenido");
		}
		catch(Exception e) {
			System.err.println("Error deteniendo el monitor: " + e.getMessage());
		}
	}

	private void ejecutarBuclePrincipal() {
		while(ejecucion) {
			mostrarMenu();
			int opcion= leerOpcion();
			procesarOpcion(opcion);
		}
		
	}

	private boolean verificarEstadoEngine() {
		try {
			Socket s= new Socket(hostEngine, puertoEngine);
			escribirDatos(s, "Comprobar_Funciona");
			
			String respuesta= leerDatos(s);
			System.out.println("Respuesta funcionamiento: " + respuesta); 
			if ("Funciona_OK".equals(respuesta)) {
				return true;
			}
			else {
				return false;
			}
		}
		catch(IOException e) {
			 System.err.println("Error verificando estado del Engine: " + e.getMessage());
			 return false;
		}
	}

	private boolean conectarCentral() {
		try {
			Properties propiedades= new Properties();
			propiedades.put("bootstrap.servers", dirKafka);
			propiedades.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			propiedades.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		    propiedades.put("acks", "1");
		    KafkaProducer<String, String> productor = new KafkaProducer<>(propiedades);
		    
		    String registroStr = String.format("Monitor_Registro|%s", cpId);
		    ProducerRecord<String, String> record = new ProducerRecord<>("monitor-registro", cpId, registroStr);
		    productor.send(record);
		    System.out.println("Monitor registrado en la Central para CP: " + cpId);
	        productor.close();
	        return true;
		}
		catch(Exception e) {
			 System.err.println("Error en la conexion con la Central: " + e.getMessage());
			 return false;
		}
	}
	
	private void reportarAveria() {
		try {
			Properties propiedades = new Properties();
			propiedades.put("bootstrap.servers", dirKafka);
			propiedades.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			propiedades.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        KafkaProducer<String, String> productor = new KafkaProducer<>(propiedades);
	        
	        String mensaje="Averia_Reporte|" +cpId;
	        ProducerRecord<String, String> record = new ProducerRecord<>("fallo-cp", cpId, mensaje);
	        productor.send(record);
	        System.out.println("Avería reportada a Central");
	        productor.close();
		}
		catch(Exception e) {
	        System.err.println("Error reportando la avería: " + e.getMessage());

		}
	}
	
	private void reportarRecuperacion() {
		try {
			Properties propiedades = new Properties();
			propiedades.put("bootstrap.servers", dirKafka);
			propiedades.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			propiedades.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        KafkaProducer<String, String> productor = new KafkaProducer<>(propiedades);
	        
	        String mensaje="Recuperacion_Reporte|" +cpId;
	        ProducerRecord<String, String> record = new ProducerRecord<>("recuperacion-cp", cpId, mensaje);
	        productor.send(record);
	        System.out.println("Recuperacion reportada a Central");
	        productor.close();
		}
		catch(Exception e) {
	        System.err.println("Error reportando la recuperacion: " + e.getMessage());

		}
	}
	
	private void mostrarMenu() {
		System.out.println("\n--- MENÚ MONITOR CP " + cpId + " ---");
        System.out.println("1. Verificar estado del Engine");
        System.out.println("2. Verificar estado seguido (cada 2 segundos)");
        System.out.println("3. Forzar reporte de avería a Central");
        System.out.println("4. Forzar reporte de recuperación a Central");
        System.out.println("5. Salir");
        System.out.print("Seleccione opción: ");
	}
	
	private int leerOpcion() {
		try {
			return scanner.nextInt();
		}
		catch(Exception e) {
			scanner.nextLine();
			System.out.println("Opción incorrecta. Introduce un número del 1 al 5.");
            return -1;
		}
	}
	
	private void procesarOpcion(int opcion) {
		switch(opcion) {
			case 1:
				verificarEstado();
				break;
			case 2:
				verificarEstadoAuto();
				break;
			case 3:
				simularAveria();
				break;
			case 4:
				simularRecuperacion();
				break;
			case 5:
				System.out.println("Saliendo del monitor...");
                ejecucion = false;
                break;
            default:
            	System.out.println("Opción incorrecta. Introduce un número del 1 al 5.");
		}
	}
	
	private void verificarEstado() {
		boolean estado= verificarEstadoEngine();
		if(estado) {
			 System.out.println("Engine funcionando correctamente");
		}
		else {
			 System.out.println("Engine funciona mal");
		}
	}
	
	//se usa una funcion lambda para que se haga cada vez que se cree un nuevo hilo
	//Con un hilo lo que hace es verificar el estado del engine cada 2 segundos y se detiene al pulsar enter, se puede seguir usando el menu mientras se ejecuta
	private void verificarEstadoAuto() {
		System.out.println("Iniciando verificación automatica del engine (presione Enter para detener)...");
		scanner.nextLine();
		
		Thread hilo = new Thread(new Runnable() {
			@Override
			public void run() {
				while(!Thread.currentThread().isInterrupted() && ejecucion) {
					try {
						boolean estado=verificarEstadoEngine();
						if (estado) {
	                        System.out.println("Engine OK - " + java.time.LocalTime.now());
	                    } 
						else {
	                        System.out.println("Engine KO - " + java.time.LocalTime.now());
	                        reportarAveria();
	                    }
						Thread.sleep(2000);
					} 
					catch (InterruptedException e) {
	                    //si no se pone nada se sigue
						break;
	                }
					catch(Exception e) {
						System.err.println("Error en verificación automatica: " + e.getMessage());
						break;
					}
				}
			}
		});
		
		hilo.start();
		
		//parte de parar con enter
		try {
			System.in.read();
			hilo.interrupt();
			System.out.println("Verificación automatica finalizada");
		}
		catch(IOException e) {
			System.err.println("Error leyendo la entrada: " + e.getMessage());
		}
	}
	
	private void simularAveria() {
		System.out.println("Simulando avería...");
        reportarAveria();
	}
	
	private void simularRecuperacion() {
        System.out.println("Simulando recuperación...");
        reportarRecuperacion();
    }
 
    
    
}
