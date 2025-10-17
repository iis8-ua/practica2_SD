package p2.evcharging.cp.network;

import p2.evcharging.cp.ChargingPoint;
import java.io.*;
import java.net.Socket;
import java.util.Properties;

public class CentralConnector {
	private String host;
	private int puerto;
	private String dirKafka;
	private ChargingPoint cp;
	private Socket socket;
	private DataOutputStream enviarMensaje; 
	private DataInputStream recibirMensaje;
	private Object kafkaProducer;
	private boolean conectado;
	
	public CentralConnector(String host, int puerto, String dirKafka, ChargingPoint cp) {
		this.host=host;
		this.puerto=puerto;
		this.dirKafka=dirKafka;
		this.cp=cp;
		configurarKafka();
	}
	
	private void configurarKafka() {
		
	}
	
	public String leerDatos(Socket sock) {
		String datos = "";
		try {
			InputStream aux= sock.getInputStream();
			DataInputStream flujo = new DataInputStream(aux);
			datos=flujo.readUTF();
		}
		catch (Exception e) {
			System.out.println("Error al leer datos " + e.toString());
		}
		return datos;
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
	
	
	public boolean registrarCentral() {
		try {
			socket = new Socket(host, puerto);
            enviarMensaje = new DataOutputStream(socket.getOutputStream());
            recibirMensaje = new DataInputStream(socket.getInputStream());
            
            
            return true;
		}
		catch(IOException e) {
			System.err.println("Error al registar el CP en la central: " + e.getMessage());
		}
		return false;
	}
	
	private void escucharComandos() {
		
	}
	
	
	public void enviarEstadoACentral() {
		if(conectado && socket !=null) {
			String funciona;
			if(cp.getFunciona()) {
				funciona ="Ok";
			}
			else {
				funciona= "Ko";
			}
			String estadoStr= String.format("Actualizacion_Estado|%s|%s|%s", cp.getId(), cp.getEstado().name(), funciona);
			escribirDatos(socket,estadoStr);
		}
		
	}

	public void enviarMensajeACentral(String mensaje) {
		if(conectado && socket !=null) {
			escribirDatos(socket, mensaje)
		}
	}

	public void enviarAutorizacion(String sesionId, String conductorId, boolean autorizado) {
		String mensaje;
		
		if(autorizado) {
			mensaje=String.format("Autorización_Aceptada|%s|%s", sesionId, conductorId);
		}
		else {
			mensaje=String.format("Autorización_Denegada|%s|%s", sesionId, conductorId);
		}
		
		enviarMensajeACentral(mensaje);
	}


	public void enviarActualizacionConsumo(double consumoActual, double importeActual) {
		String mensaje=String.format("Actualización_Consumo|%s|%.2f|%.2f", cp.getId(), consumoActual, importeActual);
		enviarEventoKafka("actualización-recarga", mensaje);
		enviarMensajeACentral(mensaje);
		
	}

	public void reportarAveria() {
		String mensaje= "Averia|";
		enviarMensajeACentral(mensaje + cp.getId());
		enviarEventoKafka("fallo-cp", mensaje + cp.getId());	
	}


	public void reportarRecuperacion() {
		String mensaje= "Recuperación|";
		enviarMensajeACentral(mensaje + cp.getId());
		enviarEventoKafka("recuperación-cp", mensaje + cp.getId());	
	}
	
	private void enviarEventoKafka(String tema, String mensaje) {
		
	}
	
	public void cerrarConexiones() {
		
	}

}
