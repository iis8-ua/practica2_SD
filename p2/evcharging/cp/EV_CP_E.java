package p2.evcharging.cp;

import p2.evcharging.cp.network.MonitorServer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.time.Duration;
import java.sql.*;
import p2.db.DBManager;

public class EV_CP_E {
	private ChargingPoint cp;
	private MonitorServer monitor;
	private boolean funcionamiento;
	private Scanner scanner;
	private String host;
	private int puerto;
	private String dirKafka;
	private Thread hilo; 
	
	public static void main(String[] args) {
		//para que no aparezcan los mensajes de kafka en la central 
    	System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "WARN");
    	System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", "ERROR");
    	System.setProperty("org.slf4j.simpleLogger.log.kafka", "ERROR");
    	System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka.clients", "WARN");
    	System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka.common", "WARN");
    	System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka.clients.network", "ERROR");
    	System.setProperty("org.slf4j.simpleLogger.log.org.slf4j", "WARN");
    	System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka.clients.consumer", "ERROR");
    	java.util.logging.Logger.getLogger("org.apache.kafka").setLevel(java.util.logging.Level.SEVERE);
		
		if (args.length < 6) {
            System.out.println("Uso: java EV_CP_E <cp_id> <ubicacion> <precio_kwh> <host:port> <dirKafka> <puerto_monitor>");
            //System.out.println("Ej: java EV_CP_E CP001 \"Calle Principal 123\" 0.15 localhost:9090 localhost:9092");
            return;
        }
		
		String cpId=args[0];
		String ubicacion=args[1];
		double precioKwh = Double.parseDouble(args[2]);
		String[] centralArgs = args[3].split(":");
		String dirKafka=args[4];
		int puertoMonitor=Integer.parseInt(args[5]);	
		
		String host=centralArgs[0];
		int puerto= Integer.parseInt(centralArgs[1]);
		
		EV_CP_E engine = new EV_CP_E();
		engine.iniciar(cpId, ubicacion, precioKwh, host, puerto, dirKafka, puertoMonitor);
	}
	
	  public void iniciar(String cpId, String ubicacion, double precioKwh, String host, int puerto, String dirKafka, int puertoMonitor) {
		  try {
			  this.host=host;
			  this.puerto=puerto;
			  this.dirKafka=dirKafka;
			  this.scanner= new Scanner(System.in);
			  
			  this.cp=new ChargingPoint(cpId, ubicacion, precioKwh);
			  boolean registro=cp.registroEnCentral(dirKafka);
			  
			  if(!registro) {
				  System.err.println("No se ha registrado el CP en la central");
				  return;
			  }
			  
			  this.monitor= new MonitorServer(cp, puertoMonitor);
			  Thread hiloMonitor = new Thread(() -> monitor.iniciar());
			  hiloMonitor.start();
			  
			  iniciarConsumidorCentral();

			  this.funcionamiento=true;
			  ejecutarBuclePrincipal();
		  }
		  catch(Exception e) {
			  System.err.println("Error en el inicio del engine: " + e.getMessage());
		  }
		  finally {
			  detener();
		  }
	  }

	private void iniciarConsumidorCentral() {
		hilo=new Thread (() -> {
			Properties propiedades = new Properties();
			propiedades.put("bootstrap.servers", dirKafka);
			propiedades.put("group.id", "engine-" + cp.getId());
			propiedades.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			propiedades.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
			
			try (KafkaConsumer<String, String> consumidor = new KafkaConsumer<>(propiedades)) {
				consumidor.subscribe(Arrays.asList("comandos-cp"));
				
				while(funcionamiento) {
					ConsumerRecords<String, String> records = consumidor.poll(Duration.ofMillis(100));
					 for (ConsumerRecord<String, String> record : records) {
		                    if (cp.getId().equals(record.key())) {
		                        procesarComandoCentral(record.value());
		                    }
					 }
				}
						
			}
			catch(Exception e) {
				System.err.println("Error en consumidor de Central: " + e.getMessage());
			}
		});
		hilo.start(); 
		
	}

	private void procesarComandoCentral(String comando) {
		String[] partes=comando.split("\\|");
		if(partes.length==0) {
			return;
		}
		
		String tipo= partes[0];
		
		switch(tipo) {
			case "Autorizacion_Solicitud":
				if(partes.length>4) {
					String driverId=partes[1];
					String sesionId=partes[2];
					String cpId=partes[3];
					procesarAutorizacionEngine(driverId, sesionId, cpId);
				}
				break;
			
			case "Inicio_Suministro":
				if(partes.length >=3) {
					String conductorId = partes[1];
	                String sesionId = partes[2];
	                iniciarSuministroAutomatico(conductorId, sesionId);
				}
				break;
				
			case "Parar":
				if (cp.getEstado() == CPState.SUMINISTRANDO) {
					System.out.println("Finalizando suministro...");
	                cp.finalizarSuministro();
	            }
	            cp.parar();
	            break;
	            
			case "Parada_Emergencia":
				if(cp.getEstado() == CPState.SUMINISTRANDO) {
					System.out.println("Finalizando suministro...");
					cp.finalizarSuministro();
				}
				cp.parar();
				break;
				
			case "Reanudar":
				cp.activar();
				break;
				
			default:
	            System.out.println("Comando no reconocido: " + tipo);

		}	
		
	}

	private void iniciarSuministroAutomatico(String conductorId, String sesionId) {
		boolean exito=cp.iniciarSuministroAutorizado(conductorId, sesionId);
		
		if(exito) {
			System.out.println("Suministro automático iniciado para: " + conductorId);
		}
		else {
			System.out.println("No se pudo iniciar el suministro automático");
		}
		
	}

	private void procesarAutorizacionEngine(String driverId, String sesionId, String cpId) {
		if(cp.getEstado() ==CPState.ACTIVADO && cp.getFunciona()) {
			//aceptada
			if(cp.getConector() !=null) {
				cp.getConector().enviarAutorizacion(sesionId, cpId, true);
			}
			 System.out.println("Autorización aceptada para driver: " + driverId);
		}
		//denegada
		else {
			if(cp.getConector() != null) {
				cp.getConector().enviarAutorizacion(sesionId, driverId, false);
				System.out.println("Autorización denegada para driver: " + driverId);
			}
		}
		
	}

	private void ejecutarBuclePrincipal() {
		while(funcionamiento) {
			mostrarMenu();
			int opcion= leerOpcion();
			procesarOpcion(opcion);
		}
	}
	
	private void mostrarMenu() {
		 System.out.println("\n--- MENÚ PRINCIPAL ---");
	        System.out.println("1.  Iniciar suministro manual");
	        System.out.println("2.  Finalizar suministro");
	        System.out.println("3.  Activar CP");
	        System.out.println("4.  Parar CP");
	        System.out.println("5.  Simular avería");
	        System.out.println("6.  Reparar avería");
	        System.out.println("7.  Estado completo");
	        System.out.println("8.  Salir");
	        System.out.print("Seleccione opción: ");
	}
	
	private int leerOpcion() {
	    try {
	        return scanner.nextInt();
	    } 
	    catch (Exception e) {
	        scanner.nextLine();
	        System.out.println("Opcion incorrecta. Introduce un número del 1 al 10.");
	        return -1;
	    }
	}
	
	private void procesarOpcion(int opcion) {
        switch (opcion) {
            case 1:
                iniciarSuministroManual();
                break;
            case 2:
                finalizarSuministro();
                break;
            case 3:
                activarCP();
                break;
            case 4:
                pararCP();
                break;
            case 5:
                simularAveria();
                break;
            case 6:
                repararAveria();
                break;
            case 7:
                mostrarEstadoCompleto();
                break;
            case 8:
                System.out.println("Saliendo...");
                funcionamiento = false;
                break;
            default:
                System.out.println("Opción incorrecta. Introduce un número del 1 al 10.");
        }
    }
	
	private void iniciarSuministroManual() {
		System.out.println("Introduce el ID del conductor: ");
		String conductorId=scanner.next();
		
		boolean exito= cp.iniciarSuministroManual(conductorId);
		
		if(exito) {
			System.out.println("Suministro manual inciado para: " + conductorId);
			System.out.println("Recarga en progreso...");
		    System.out.println("Pulse 2 para finalizar el suministro");
		}
		else {
			//System.out.println("No se ha podido iniciar el suministro manual");
		}
	}
	
	private void finalizarSuministro() {
		if (cp.getEstado() != CPState.SUMINISTRANDO) {
            System.out.println("No hay suministros trabajando en este momento");
            return;
        }
		
		System.out.println("Finalizando suministro para conductor: " + cp.getConductorActual());
		cp.finalizarSuministro();
		System.out.println("Suministro finalizado");
		
	}
	
	private void activarCP() {
		System.out.println("Activando CP con id: " + cp.getId());
		cp.activar();
		//System.out.println("CP activado");
	}
	
	private void pararCP() {
		System.out.println("Parando CP con id: " + cp.getId());
		cp.parar();
		//System.out.println("CP fuera de servicio");
	}
	
	private void simularAveria() {
		if(cp.getFunciona()) {
			System.out.println("Fallo en el CP " + cp.getId());
			cp.setFunciona(false);
			System.out.println("CP en estado de fallo, se ha notificado la averia a la Central");
		}
		else {
			System.out.println("El CP ya esta averiado: " +cp.getEstado());
		}
	}
	
	private void repararAveria() {
		if(cp.getEstado()==CPState.AVERIADO || !cp.getFunciona()) {
			System.out.println("Reparando averia en CP " + cp.getId());
			cp.setFunciona(true);
			System.out.println("Averia reparada, se ha notificado la recuperación a la Central");
		}
		else {
			System.out.println("No hay averia en el CP: " +cp.getEstado());
		}
	}
	
	private void mostrarEstadoCompleto() {
		cp.imprimirInfoCP();
	}

	
	private void detener() {
		funcionamiento =false;
		try {
			if(scanner != null) {
				scanner.close();
			}
			
			if(monitor != null) {
				monitor.detener();
			}
			
			Thread.sleep(1000);
			
			if(hilo !=null && hilo.isAlive()) {
				hilo.interrupt();
				hilo.join(2000);
			}
			
			if(cp !=null && cp.getConector() != null) {
				cp.getConector().cerrarConexiones();
			}
						
			resetearCPenBD();
			
			System.out.println("Engine detenido");
		}
		catch(Exception e) {
			System.out.println("Error, no se ha podido detener el engine: " + e.getMessage());
		}
	}
	
	private void resetearCPenBD() {
	    if(cp != null) {
	        try (Connection conn = DBManager.getConnection();
	             PreparedStatement ps = conn.prepareStatement(
	                     "UPDATE charging_point SET estado='DESCONECTADO', funciona=TRUE, registrado_central=FALSE, conductor_actual=NULL, consumo_actual=0.0, importe_actual=0.0 WHERE id=?")) {
	            ps.setString(1, cp.getId());
	            ps.executeUpdate();
	        } 
	        catch (SQLException e) {
	            System.err.println("[DB] Error resetando CP en BD: " + e.getMessage());
	        }
	    }
	}

	public ChargingPoint getChargingPoint() {
        return cp;
    }

    public MonitorServer getMonitorService() {
        return monitor;
    }

    public boolean getFuncionamiento() {
        return funcionamiento;
    }
}
