package p2.evcharging.cp;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;

import p2.evcharging.cp.network.CentralConnector;
import p2.evcharging.cp.service.ChargingSessionService;

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
    
    private CentralConnector conector;
    private ChargingSessionService servicio;
    
    public ChargingPoint(String id, String ubicacion, double precioKwh) {
    	this.id=id;
    	this.ubicacion=ubicacion;
    	this.precioKwh=precioKwh;
    	this.estado=CPState.DESCONECTADO;
    	this.funciona=true;
    	this.registradoCentral=false;
    	this.servicio=new ChargingSessionService(this);
    	this.consumoActual=0.0;
    	this.importeActual=0.0;
    }
   
    
   public boolean registroEnCentral(String centralHost, int centralPort) {
	   try {
	   } 
	   catch (IOException e) {
		   System.err.println("ERROR conectando a central: " + e.getMessage());
		   this.estado=CPState.DESCONECTADO;
		   return false;
	   }
   }
   
   
   public void activar() {
	   if(registradoCentral && funciona) {
		   this.estado=CPState.ACTIVADO;
		   conector.enviarEstadoACentral();
		   System.out.println("CP activado");
	   }
	   else {
		   System.out.println("No es posible la activación por averia o no esta registrado en la central");
	   }
   }
   
   public void parar() {
	   if(estado == CPState.SUMINISTRANDO) {
		   servicio.finalizarSuministro();
	   }
	   this.estado=CPState.PARADO;
	   conector.enviarEstadoACentral();
	   System.out.println("CP fuera de servicio");
   }
   
   
  public boolean iniciarSuministroManual(String conductorId) {
	  if(puedeIniciarSuministro()) {
		  return servicio.iniciarSuministro(conductorId, "Manual");
	  }
	  else {
		  System.out.println("No se puede iniciar suministro - Estado: " + estado + ", Salud: " + funciona);
          return false;
	  }
  }
  
  public boolean autorizarSuministro(String conductorId, String sesionId) {
	  if(puedeIniciarSuministro()) {
		  System.out.println("Autorizado el suministro para el conductor: " + conductorId);
		  conector.enviarAutorizacion(sesionId, conductorId, true);
		  return true;
	  }
	  else {
		  System.out.println("Denegado el suministro --> No disponible");
		  conector.enviarAutorizacion(sesionId, conductorId, false);
		  return false;
	  }
  }
  
  public boolean iniciarSuministroAutorizado(String conductorId, String sesionId) {
	  if(autorizarSuministro(conductorId, sesionId)) {
		  return servicio.iniciarSuministro(conductorId, "Automatico");
	  }
	  return false;
  }
  
  private boolean puedeIniciarSuministro() {
      return estado == CPState.ACTIVADO && funciona && registradoCentral;
  }
  
  public void actualizarConsumo(double kw) {
	   if (estado == CPState.SUMINISTRANDO) {
		   this.consumoActual += kw;
		   this.importeActual = this.consumoActual * precioKwh;
		   conector.enviarActualizacionConsumo(consumoActual,importeActual);
	   }
  }
  
  public void finalizarSuministro() {
	  if(this.estado == CPState.SUMINISTRANDO) {
		  servicio.finalizarSuministro();
		  this.consumoActual=0.0;
		  this.importeActual=0.0;
	  }
  }
  
  
  public void setFunciona(boolean funciona) {
	  boolean anterior=this.funciona;
	  this.funciona=funciona;
	  
	  if(!funciona && anterior) {
		  this.estado=CPState.AVERIADO;
		  conector.reportarAveria();
		  System.out.println("Avería pasada a Central");
		  
		  if (estado == CPState.SUMINISTRANDO) {
			  finalizarSuministro();
		  }
	  }
	  else if (funciona && !anterior) {
		  this.estado=CPState.ACTIVADO;
		  conector.reportarRecuperacion();
		  System.out.println("Recuperación pasada a Central");
	  }
  }
  
  public void procesarComandoCentral(String comando) {
	  String[] partes = comando.split("\\|");
	  String tipo = partes[0];
	  
	  switch(tipo) {
	  	case "Inicio":
	  		if(partes.length >= 3) {
	  			iniciarSuministroAutorizado(partes[1], partes[2]);
	  		}
	  		break;
	  		
	  	case "Parar":
	  		parar();
	  		break;
	  		
	  	case "Continuar":
	  		activar();
	  		break;
	  		
	  	case "Parada_Emergencia":
	  		if(estado == CPState.SUMINISTRANDO) {
	  			finalizarSuministro();
	  		}
	  		parar();
	  		break;
	  }	  
  }
  
  public void setEstado(CPState estado) {
	  this.estado=estado;
  }
  
  public void setConductorActual(String conductor) {
		this.conductorActual=conductor;	
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
   
   public ChargingSessionService getServicio() {
	   return servicio;
   }
   
   public CentralConnector getConector() {
	   return conector;
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


