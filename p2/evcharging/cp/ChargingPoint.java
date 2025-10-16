package p2.evcharging.cp;

public class ChargingPoint {
	private String id;
    private String ubicacion;
    private double precioKwh;
    private CPState estado;
    private double consumoActual;
    private double importeActual;
    private String conductorActual;
    private boolean saludOK;
}
