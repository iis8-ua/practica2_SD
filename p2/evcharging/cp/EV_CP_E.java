package p2.evcharging.cp;

import p2.evcharging.cp.network.MonitorServer;
import java.util.Scanner;

public class EV_CP_E {
	private ChargingPoint cp;
	private MonitorServer monitor;
	private boolean funcionamiento;
	private Scanner scanner;
	private String host;
	private int puerto;
	private String dirKafka;
	
	public static void main(String[] args) {
		if (args.length < 5) {
            System.out.println("Uso: java EV_CP_E <cp_id> <ubicacion> <precio_kwh> <host:port> <dirKafka>");
            //System.out.println("Ej: java EV_CP_E CP001 \"Calle Principal 123\" 0.15 localhost:9090 localhost:9092");
            return;
        }
		
		String cpId=args[0];
		String ubicacion=args[1];
		double precioKwh = Double.parseDouble(args[2]);
		String[] centralArgs = args[3].split(":");
		String dirKafka=args[4];
		
		String host=centralArgs[0];
		int puerto= Integer.parseInt(centralArgs[1]);
		
		EV_CP_E engine = new EV_CP_E();
		engine.iniciar(cpId, ubicacion, precioKwh, host, puerto, dirKafka);
	}
	
	  public void iniciar(String cpId, String ubicacion, double precioKwh, String host, int puerto, String dirKafka) {
		  try {
			  this.host=host;
			  this.puerto=puerto;
			  this.dirKafka=dirKafka;
			  this.scanner= new Scanner(System.in);
			  
			  this.cp=new ChargingPoint(cpId, ubicacion, precioKwh);
			  boolean registro=cp.registroEnCentral(host, puerto, dirKafka);
			  
			  if(!registro) {
				  System.err.println("No se ha registrado el CP en la central");
				  return;
			  }
			  
			  this.monitor= new MonitorServer(cp, 8080);
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

	private void ejecutarBuclePrincipal() {
		while(funcionamiento) {
			mostrarMenu();
			int opcion= leerOpcion();
			procesarOpcion(opcion);
		}
	}
	
	private void mostrarMenu() {
		 System.out.println("\n--- MENÚ PRINCIPAL ---");
	        System.out.println("1.  Iniciar suministro MANUAL");
	        System.out.println("2.  Finalizar suministro");
	        System.out.println("3.  Activar CP");
	        System.out.println("4.  Parar CP");
	        System.out.println("5.  Simular avería");
	        System.out.println("6.  Reparar avería");
	        System.out.println("7.  Estado completo");
	        //System.out.println("8.  Verificar conexión con Central");
	        //System.out.println("9.  Enviar test a Kafka");
	        System.out.println("8. Salir");
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
            /*case 8:
                verificarConexion();
                break;
            case 9:
                enviarTestKafka();
                break;*/
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
		}
		else {
			System.out.println("No se ha podido iniciar el suministro manual");
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
		System.out.println("CP activado");
	}
	
	private void pararCP() {
		System.out.println("Parando CP con id: " + cp.getId());
		cp.parar();
		System.out.println("CP fuera de servicio");
	}
	
	private void simularAveria() {
		System.out.println("Fallo en el CP " + cp.getId());
		cp.setFunciona(false);
		System.out.println("CP en estado de fallo, se ha notificado la averia a la Central");
	}
	
	private void repararAveria() {
		System.out.println("Reparando averia en CP " + cp.getId());
		cp.setFunciona(true);
		System.out.println("Averia reparada, se ha notificado la recuperación a la Central");
	}
	
	private void mostrarEstadoCompleto() {
		cp.imprimirInfoCP();
	}
	
	/*private void verificarConexion() {
        System.out.println("⏳ Comprobando estado de la conexión...");
        
        if (cp.getConector().estaConectado()) {
            System.out.println("Conectado a Central");
        } else {
            System.out.println("Desconectado de Central");
        }
    }
    
    private void enviarTestKafka() {
        System.out.println("Enviando mensaje de prueba a Kafka...");
        cp.getConector().enviarActualizacionConsumo(2.5, 0.375);
        System.out.println("Mensaje de prueba enviado a Kafka");
    }*/
	
	private void detener() {
		funcionamiento =false;
		try {
			if(scanner != null) {
				scanner.close();
			}
			
			if(monitor != null) {
				monitor.detener();
			}
			
			if(cp !=null && cp.getConector() != null) {
				cp.getConector().cerrarConexiones();
			}
			
			System.out.println("Engine detenido");
		}
		catch(Exception e) {
			System.out.println("Error, no se ha podido detener el engine: " + e.getMessage());
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
