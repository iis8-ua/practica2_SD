package p2.evcharging.cp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public class ChargingPoint {
	private String id;
    private String ubicacion;
    private double precioKwh;
    private CPState estado; 
    /*
     El consumo actual (en tiempo real), el importe actual, y el id del conductor se ponen cuando 
     el estado del punto de recarga es de Suministrando, cuando no es ese no se asignan valores
     */
    private double consumoActual;
    private double importeActual;
    private String conductorActual;
    //Esta variable está a true o false dependiendo si funciona o no el CP
    private boolean funciona;
    private boolean registradoCentral;
    private Socket centralSocket;
    private PrintWriter centralOut;
    private BufferedReader centralIn;
    
    public ChargingPoint(String id, String ubicacion, double precioKwh) {
    	this.id=id;
    	this.ubicacion=ubicacion;
    	this.precioKwh=precioKwh;
    	this.estado=CPState.DESCONECTADO;
    	this.funciona=true;
    	this.registradoCentral=false;
    }
   
    
   public boolean registroEnCentral(String centralHost, int centralPort) {
	   try {
		centralSocket = new Socket(centralHost, centralPort);
		centralOut= new PrintWriter(centralSocket.getOutputStream(), true);
		InputStreamReader inp= new InputStreamReader(centralSocket.getInputStream());
		centralIn = new BufferedReader(inp);
		
		//falta crear la central primero
		
		return true;
	   } 
	   catch (IOException e) {
		   System.err.println("ERROR conectando a central: " + e.getMessage());
		   this.estado=CPState.DESCONECTADO;
		   return false;
	   }
   }
   
   private void enviarEstadoACentral() {
   	
   }
   
   private void enviarMensajeACentral(String mensaje) {
   	
   }
   
   public void activar() {
	   if(registradoCentral && funciona) {
		   this.estado=CPState.ACTIVADO;
		   enviarEstadoACentral();
		   System.out.println("CP activado");
	   }
	   else {
		   System.out.println("No es posible la activación por averia o no esta registrado en la central");
	   }
   }
   
   public void parar() {
	   if(estado == CPState.SUMINISTRANDO) {
		   finalizarSuministro();
	   }
	   this.estado=CPState.PARADO;
	   enviarEstadoACentral();
	   System.out.println("CP fuera de servicio");
   }
   
   
  public boolean iniciarSuministroManual(String conductorId) {
	  if(estado==CPState.SUMINISTRANDO && funciona && registradoCentral) {
		  return iniciarSuministro(conductorId, "Manual");
	  }
	  else {
		  System.out.println("No se puede iniciar suministro - Estado: " + estado + ", Salud: " + funciona);
          return false;
	  }
  }
  
  public boolean autorizarSuministro(String conductorId, String sesionId) {
	  
  }
  
  public boolean iniciarSuministroAutorizado(String conductorId, String sesionId) {
	  if(autorizarSuministro(conductorId, sesionId)) {
		  return iniciarSuministro(conductorId, "Automatico");
	  }
	  return false;
  }
   
  public boolean iniciarSuministro(String conductorId, String tipo) {
	   this.estado=CPState.SUMINISTRANDO;
	   this.conductorActual=conductorId;
	   this.consumoActual=0.0;
	   this.importeActual=0.0;
	   
	   enviarMensajeACentral();
	   return true;
  }
  
  public void actualizarConsumo(double kw) {
	   if (estado == CPState.SUMINISTRANDO) {
		   this.conductorActual += kw;
		   this.importeActual = this.consumoActual * precioKwh;
		   enviarMensajeACentral();
	   }
  }
  
  public void finalizarSuministro() {
	   if(estado==CPState.SUMINISTRANDO) {
		   enviarMensajeACentral();
		   System.out.println("Suministro finalizado");
	       System.out.println("Ticket - Consumo: " + consumoActual + " kW, Importe: " + importeActual + " €");
	       
	       this.estado = CPState.ACTIVADO;
	       this.conductorActual = null;
	       this.consumoActual = 0.0;
	       this.importeActual = 0.0;
	   }
  }
  
  public void setFunciona(boolean funciona) {
	  boolean anterior=this.funciona;
	  this.funciona=funciona;
	  
	  if(!funciona && anterior) {
		  this.estado=CPState.AVERIADO;
		  enviarMensajeACentral(id);
		  System.out.println("Avería pasada a Central");
		  
		  if (estado == CPState.SUMINISTRANDO) {
			  finalizarSuministro();
		  }
	  }
	  else if (funciona && !anterior) {
		  this.estado=CPState.ACTIVADO;
		  enviarMensajeACentral(id);
		  System.out.println("Recuperación pasada a Central");
	  }
  }
  
  public void procesarComandoCentral(String comando) {
	  
  }
    
   public String getId() { 
	   return id; 
   }
   public String getUbicacion() { 
	   return ubicacion; 
   }
   public double getPrecioKwh() { 
	   return precioKwh; 
   }
   public CPState getEstado() { 
	   return estado; 
   }
   public double getConsumoActual() { 
	   return consumoActual; 
   }
   public double getImporteActual() { 
	   return importeActual; 
   }
   public String getConductorActual() { 
	   return conductorActual; 
   }
   public boolean getFunciona() { 
	   return funciona; 
   }
   public boolean getRegistradoCentral() { 
	   return registradoCentral; 
   }
   
   public void imprimirInfoCP() {
	   System.out.println("\n=== ESTADO COMPLETO CP " + id + " ===");
       System.out.println("Ubicación: " + ubicacion);
       System.out.println("Precio: " + precioKwh + " €/kWh");
       System.out.println("Estado: " + estado + " (" + estado.getColor() + ")");
       
       if(funciona) {
    	   System.out.println("Salud: OK");
       }
       else {
    	   System.out.println("Salud: AVERIADO");
       }
       
       if(registradoCentral) {
    	   System.out.println("Registrado: SÍ");
       }
       else {
    	   System.out.println("Registrado: NO");
       }
       
       if (estado == CPState.SUMINISTRANDO) {
           System.out.println("Conductor: " + conductorActual);
           System.out.println("Consumo actual: " + consumoActual + " kW");
           System.out.println("Importe actual: " + importeActual + " €");
       }
       System.out.println("=================================");
   }
  
    
    
}


