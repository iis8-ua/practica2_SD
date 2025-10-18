package p2.evcharging.cp.network;

import p2.evcharging.cp.ChargingPoint;
import java.io.*;
import java.net.Socket;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class CentralConnector {
	private String host;
	private int puerto;
	private String dirKafka;
	private ChargingPoint cp;
	private Socket socket;
	private DataOutputStream enviarMensaje; 
	private DataInputStream recibirMensaje;
	private KafkaProducer<String, String> kafkaProducer; 
	private boolean conectado;
	
	public CentralConnector(String host, int puerto, String dirKafka, ChargingPoint cp) {
		this.host=host;
		this.puerto=puerto;
		this.dirKafka=dirKafka;
		this.cp=cp;
		this.conectado=false;
		configurarKafka();
	}
	
	private void configurarKafka() {
		try {
			Properties props = new Properties();
			props.put("bootstrap.servers", dirKafka);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("acks", "1");
            props.put("retries", 3);
            this.kafkaProducer = new KafkaProducer<>(props);
			System.out.println("Kafka configurado correctamente");
		}
		catch(Exception e) {
			System.err.println("Error configurado correctamente " + e.getMessage());
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
            
            String registroStr = String.format("Registro|%s|%s|%.2f", cp.getId(), cp.getUbicacion(), cp.getPrecioKwh());
            escribirDatos(socket, registroStr);
            
            String respuesta= leerDatos(socket);
            if("Registro_OK".equals(respuesta)) {
            	this.conectado=true;
            	System.out.println("Registrado en Central correctamente");
            	return true;
            }
            else {
            	System.out.println("Registro en Central fallido");
            	return false;
            }

		}
		catch(IOException e) {
			System.err.println("Error al registar el CP en la central: " + e.getMessage());
			return false;
		}
		
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
			enviarEventoKafka("cp-estado", estadoStr);
			
			String respuesta=leerDatos(socket);
			if (respuesta != null && respuesta.startsWith("Actualizacion_OK")) {
				System.out.println("Estado actualizado en la Central");
			}
			else {
				System.out.println("Error actualizando el estado: " + respuesta);
			}
		}
		
	}

	public void enviarAutorizacion(String sesionId, String conductorId, boolean autorizado) {
		if(conectado && socket != null) {
			String mensaje;
			
			if(autorizado) {
				mensaje=String.format("Autorización_Aceptada|%s|%s", sesionId, conductorId);
			}
			else {
				mensaje=String.format("Autorización_Denegada|%s|%s", sesionId, conductorId);
			}
			
			escribirDatos(socket, mensaje);
			enviarEventoKafka("cp-autorizacion", mensaje);
			
			String respuesta = leerDatos(socket);
			if (respuesta != null && (respuesta.startsWith("Autorización_OK_ACK") || respuesta.startsWith("Autorización_DEN_ACK"))) {
				System.out.println("Actualización confirmada en la Central");
			}
			else {
				System.out.println("Error en la autorizacion: " + respuesta);
			}
		}
		
		
	}


	public void enviarActualizacionConsumo(double consumoActual, double importeActual) {
		if(conectado && socket !=null) {
			String mensaje=String.format("Actualización_Consumo|%s|%.2f|%.2f", cp.getId(), consumoActual, importeActual);
			escribirDatos(socket, mensaje);
			enviarEventoKafka("actualización-recarga", mensaje);
			
			String respuesta = leerDatos(socket);
			if (respuesta != null && respuesta.startsWith("Consumo_OK")) {
				System.out.println("Consumo actualizado en la Central");
			}
			else {
				System.out.println("Error en la autorizacion del consumo: " + respuesta);
			}
		}
		
	}

	public void reportarAveria() {
		if (conectado && socket !=null) {
			String mensaje= "Averia|" + cp.getId();
			escribirDatos(socket, mensaje);
			enviarEventoKafka("fallo-cp", mensaje);	
			
			String respuesta=leerDatos(socket);
			if (respuesta != null && respuesta.startsWith("Averia_ACK")) {
				System.out.println("Averia pasada a la Central");
			}
			else {
				System.out.println("Error pasando la averia: " + respuesta);
			}
		}
	}

	public void reportarRecuperacion() {
		if(conectado && socket != null) {
			String mensaje= "Recuperación|"+cp.getId();
			escribirDatos(socket, mensaje);
			enviarEventoKafka("recuperación-cp", mensaje);	
			
			String respuesta=leerDatos(socket);
			if (respuesta != null && respuesta.startsWith("Recuperacion_ACK")) {
				System.out.println("Recuperacion pasada a la Central");
			}
			else {
				System.out.println("Error pasando la recuperación: " + respuesta);
			}
		}
	}
	
	private void enviarEventoKafka(String tema, String mensaje) {
		if(kafkaProducer == null) {
			System.out.println("No esta disponible el productor");
			return;
		}
		
		try {
			ProducerRecord<String, String> record= new ProducerRecord<>(tema, cp.getId(), mensaje);
			kafkaProducer.send(record);
			System.out.println("Evento enviado a Kafka --> Tema: " + tema + ", Mensaje: " + mensaje);
		}
		catch(Exception e) {
			System.err.println("Error de Kafka en el envio del evento a la central: " + e.getMessage());
		}
		
	}
	
	public boolean estaConectado() {
		return conectado && socket !=null && !socket.isClosed();
	}
	
	public void cerrarConexiones() {
		conectado=false;
		try {
			
		}
		catch(Exception e) {
			System.err.println("Error cerrando conexiones: " +e.getMessage());
		}
	}

}
