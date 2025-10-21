package p2.evcharging.cp;

import p2.evcharging.cp.network.CentralConnector;
import java.io.*;
import java.net.Socket;
import java.util.Scanner;


public class EV_CP_M {
	private String cpId;
    private String hostEngine;
    private int puertoEngine;
    private String hostCentral;
    private int puertoCentral;
    private String dirKafka;
    private boolean ejecucion;
    private Thread hilo;
    private CentralConnector conector;
    private Scanner scanner;
    
    
    public static void main(String[] args) {
        if (args.length < 6) {
            System.out.println("Uso: java EV_CP_M <host_engine> <puerto_engine> <host_central> <puerto_central> <cp_id> <dirKafka>");
            //System.out.println("Ej: java EV_CP_M localhost 8080 localhost 9090 CP001 localhost:9092");
            return;
        }

        String hostEngine = args[0];
        int puertoEngine = Integer.parseInt(args[1]);
        String hostCentral = args[2];
        int puertoCentral = Integer.parseInt(args[3]);
        String cpId = args[4];
        String dirKafka = args[5];

        EV_CP_M monitor = new EV_CP_M();
        monitor.iniciar(hostEngine, puertoEngine, hostCentral, puertoCentral, cpId, dirKafka);
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
    
    public void iniciar(String hostEngine, int puertoEngine, String hostCentral, int puertoCentral, String cpId, String dirKafka) {
    	try {
    		this.hostEngine = hostEngine;
            this.puertoEngine = puertoEngine;
            this.hostCentral = hostCentral;
            this.puertoCentral = puertoCentral;
            this.cpId = cpId;
            this.dirKafka = dirKafka;
            this.ejecucion = true;
            this.scanner = new Scanner(System.in);
            
            if (!conectarCentral()) {
            	System.err.println("No se ha podido establecer conecion con la central. Saliendo...");
                return;
            }
            
            iniciarMonitorizacion();
            System.out.println("Monitor iniciado para el CP: " + cpId);
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
			if(hilo !=null && hilo.isAlive()) {
				hilo.interrupt();
			}
			
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

	private void iniciarMonitorizacion() {
		hilo = new Thread(() -> {
			while(ejecucion) {
				try {
					boolean estadoEngine=verificarEstadoEngine();
					
					if(!estadoEngine) {
						reportarAveria();
						System.out.println("Averia producida");
					}
					else {
						System.out.println("Engine funcionando");
					}
				}
				
				catch(Exception e) {
					System.err.println("Error en la monitorización: " + e.getMessage());
				}
			}
		});
		hilo.start();
		
	}

	private boolean verificarEstadoEngine() {
		try {
			Socket s= new Socket(hostEngine, puertoEngine);
			escribirDatos(s, "Comprobar_Funciona");
			
			String respuesta= leerDatos(s);
			if (respuesta == "Funciona_OK") {
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
			ChargingPoint temp= new ChargingPoint(cpId, "Temporal", 0.15);
			this.conector = new CentralConnector(hostCentral, puertoCentral, dirKafka, temp);
			boolean exito= conector.registrarCentral();
			
			if(exito) {
				System.out.println("Monitor registrado en la Central para CP: " + cpId);
                return true;
            } 
			else {
                System.err.println("Error en la monitorización del CP con la Central");
                return false;
			}
		}
		catch(Exception e) {
			 System.err.println("Error en la conexion con la Central: " + e.getMessage());
			 return false;
		}
	}
	
	private void reportarAveria() {
		if(conector !=null) {
			System.out.println("Reportando avería a la Central...");
			conector.reportarAveria();
		}
	}
	
	private void reportarRecuperacion() {
		if(conector !=null) {
			System.out.println("Reportando recuperación a la Central...");
			conector.reportarRecuperacion();
		}
	}
	
	private void mostrarMenu() {
		System.out.println("\n--- MENÚ MONITOR CP " + cpId + " ---");
        System.out.println("1. Verificar estado del Engine");
        System.out.println("2. Forzar reporte de avería a Central");
        System.out.println("3. Forzar reporte de recuperación a Central");
        System.out.println("4. Salir");
        System.out.print("Seleccione opción: ");
	}
	
	private int leerOpcion() {
		try {
			return scanner.nextInt();
		}
		catch(Exception e) {
			scanner.nextLine();
			System.out.println("Opción incorrecta. Introduce un número del 1 al 4.");
            return -1;
		}
	}
	
	private void procesarOpcion(int opcion) {
		switch(opcion) {
			case 1:
				verificarEstado();
				break;
			case 2:
				simularAveria();
				break;
			case 3:
				simularRecuperacion();
				break;
			case 4:
				System.out.println("Saliendo del monitor...");
                ejecucion = false;
                break;
            default:
            	System.out.println("Opción incorrecta. Introduce un número del 1 al 4.");
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
	
	private void simularAveria() {
		System.out.println("Simulando avería...");
        reportarAveria();
	}
	
	private void simularRecuperacion() {
        System.out.println("Simulando recuperación...");
        reportarRecuperacion();
    }
 
    
    
}
