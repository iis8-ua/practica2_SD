package p2.evcharging.cp;

import p2.evcharging.cp.network.CentralConnector;
import java.io.*;
import java.net.Socket;
import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.sql.*;

import p2.db.DBManager;

/**
 * EV_CP_M (Monitor) - versión extendida con integración a BD.
 * 
 * Mantiene toda la funcionalidad original,
 * y sincroniza el estado del monitor en la base de datos (tabla monitor + event_log).
 */
public class EV_CP_M {
    private String cpId;
    private String hostEngine;
    private int puertoEngine;
    private String dirKafka;
    private boolean ejecucion;
    private CentralConnector conector;
    private Scanner scanner;

    public static void main(String[] args) {
        // Reducir ruido de logs Kafka
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "WARN");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.kafka", "WARN");
        java.util.logging.Logger.getLogger("org.apache.kafka").setLevel(java.util.logging.Level.SEVERE);

        if (args.length < 4) {
            System.out.println("Uso: java EV_CP_M <host_engine> <puerto_engine> <cp_id> <dirKafka>");
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
            OutputStream aux = sock.getOutputStream();
            DataOutputStream flujo = new DataOutputStream(aux);
            flujo.writeUTF(datos);
        } catch (Exception e) {
            System.out.println("Error al escribir datos: " + e.toString());
        }
    }

    public String leerDatos(Socket sock) {
        String datos = "";
        try {
            InputStream aux = sock.getInputStream();
            DataInputStream flujo = new DataInputStream(aux);
            datos = flujo.readUTF();
        } catch (EOFException eof) {
            return null;
        } catch (IOException e) {
            System.out.println("Error al leer datos: " + e.toString());
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

            DBManager.connect();

            if (!conectarCentral()) {
                System.err.println("No se ha podido establecer conexión con la central. Saliendo...");
                actualizarMonitorBD(false);
                return;
            }

            System.out.println("Monitor iniciado para el CP: " + cpId);
            System.out.println("Conectado al Engine en: " + hostEngine + ":" + puertoEngine);
            System.out.println("Conectado a Central en: " + dirKafka);

            actualizarMonitorBD(true);
            registrarEvento("REGISTRO_MONITOR", "Monitor activo y conectado");

            ejecutarBuclePrincipal();
        } catch (Exception e) {
            System.err.println("Error en el inicio del monitor: " + e.getMessage());
        } finally {
            detener();
        }
    }

    private void detener() {
        ejecucion = false;
        try {
            if (conector != null) {
                conector.cerrarConexiones();
            }
            if (scanner != null) {
                scanner.close();
            }
            actualizarMonitorBD(false);
            registrarEvento("CONFIRMACION", "Monitor detenido correctamente");
            System.out.println("Monitor detenido");
        } catch (Exception e) {
            System.err.println("Error deteniendo el monitor: " + e.getMessage());
        }
    }

    private void ejecutarBuclePrincipal() {
        while (ejecucion) {
            mostrarMenu();
            int opcion = leerOpcion();
            procesarOpcion(opcion);
        }
    }

    private boolean verificarEstadoEngine() {
        try {
            Socket s = new Socket(hostEngine, puertoEngine);
            escribirDatos(s, "Comprobar_Funciona");
            String respuesta = leerDatos(s);
            s.close();

            System.out.println("Respuesta funcionamiento: " + respuesta);
            return "Funciona_OK".equals(respuesta);
        } catch (IOException e) {
            System.err.println("Error verificando estado del Engine: " + e.getMessage());
            return false;
        }
    }

    private boolean conectarCentral() {
        try {
            Properties propiedades = new Properties();
            propiedades.put("bootstrap.servers", dirKafka);
            propiedades.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            propiedades.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            propiedades.put("acks", "1");

            KafkaProducer<String, String> productor = new KafkaProducer<>(propiedades);
            String registroStr = String.format("Monitor_Registro|%s", cpId);
            ProducerRecord<String, String> record = new ProducerRecord<>("monitor-registro", cpId, registroStr);
            productor.send(record);

            productor.close();

            System.out.println("Monitor registrado en la Central para CP: " + cpId);
            registrarEvento("REGISTRO_MONITOR", "Monitor registrado en la Central");
            actualizarMonitorBD(true);
            return true;
        } catch (Exception e) {
            System.err.println("Error en la conexión con la Central: " + e.getMessage());
            registrarEvento("AUTORIZACION_DENEGADA", "Fallo al registrar monitor");
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

            String mensaje = "Averia_Reporte|" + cpId;
            ProducerRecord<String, String> record = new ProducerRecord<>("fallo-cp", cpId, mensaje);
            productor.send(record);
            productor.close();

            System.out.println("Avería reportada a Central");
            registrarEvento("AVERIA", "Avería reportada por monitor");
            actualizarMonitorBD(false);
        } catch (Exception e) {
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

            String mensaje = "Recuperacion_Reporte|" + cpId;
            ProducerRecord<String, String> record = new ProducerRecord<>("recuperacion-cp", cpId, mensaje);
            productor.send(record);
            productor.close();

            System.out.println("Recuperación reportada a Central");
            registrarEvento("RECUPERACION", "Recuperación reportada por monitor");
            actualizarMonitorBD(true);
        } catch (Exception e) {
            System.err.println("Error reportando la recuperación: " + e.getMessage());
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
        } catch (Exception e) {
            scanner.nextLine();
            System.out.println("Opción incorrecta. Introduce un número del 1 al 5.");
            return -1;
        }
    }

    private void procesarOpcion(int opcion) {
        switch (opcion) {
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
        boolean estado = verificarEstadoEngine();
        if (estado) {
            System.out.println("Engine funcionando correctamente");
            actualizarMonitorBD(true);
        } else {
            System.out.println("Engine funciona mal");
            actualizarMonitorBD(false);
        }
    }

    private void verificarEstadoAuto() {
        System.out.println("Iniciando verificación automática del engine (presione Enter para detener)...");
        scanner.nextLine();

        Thread hilo = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted() && ejecucion) {
                try {
                    boolean estado = verificarEstadoEngine();
                    if (estado) {
                        System.out.println("Engine OK - " + java.time.LocalTime.now());
                        actualizarMonitorBD(true);
                    } else {
                        System.out.println("Engine KO - " + java.time.LocalTime.now());
                        reportarAveria();
                    }
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    System.err.println("Error en verificación automática: " + e.getMessage());
                    break;
                }
            }
        });

        hilo.start();

        try {
            System.in.read();
            hilo.interrupt();
            System.out.println("Verificación automática finalizada");
        } catch (IOException e) {
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

    // --- Métodos auxiliares BD ---

    private void actualizarMonitorBD(boolean conectado) {
        try (Connection conn = DBManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE monitor SET conectado=?, fecha_registro=NOW() WHERE cp_id=?")) {
            ps.setBoolean(1, conectado);
            ps.setString(2, cpId);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] Error actualizando monitor: " + e.getMessage());
        }
    }

    private void registrarEvento(String tipo, String descripcion) {
        try (Connection conn = DBManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO event_log (cp_id, tipo_evento, descripcion) VALUES (?, ?, ?)")) {
            ps.setString(1, cpId);
            ps.setString(2, tipo);
            ps.setString(3, descripcion);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] Error registrando evento monitor: " + e.getMessage());
        }
    }
}
