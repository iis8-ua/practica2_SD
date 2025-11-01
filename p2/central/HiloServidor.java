package p2.central;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.time.Duration;
import java.sql.*;
import p2.db.DBManager;

/**
 * HiloServidor - versión extendida con integración a base de datos
 * 
 * Mantiene toda la lógica original de procesamiento Kafka,
 * y agrega inserciones en tablas event_log, charging_point, charging_session, etc.
 */
public class HiloServidor extends Thread {
    private KafkaProducer<String, String> productor;
    private KafkaConsumer<String, String> consumidor;
    private boolean ejecucion;

    public HiloServidor(KafkaProducer<String, String> productor, KafkaConsumer<String, String> consumidor) {
        this.productor = productor;
        this.consumidor = consumidor;
        this.ejecucion = false;
    }

    @Override
    public void run() {
        this.ejecucion = true;
        procesarMensajesKafka();
    }

    private void procesarMensajesKafka() {
        while (ejecucion) {
            try {
                ConsumerRecords<String, String> records = consumidor.poll(Duration.ofMillis(100));
                records.forEach(record -> new Thread(() ->
                        procesarMensaje(record.topic(), record.key(), record.value())).start());
            } catch (Exception e) {
                if (ejecucion) {
                    System.err.println("Error procesando los mensajes de Kafka: " + e.getMessage());
                }
            }
        }
    }

    private void procesarMensaje(String tema, String cpId, String mensaje) {
        try {
            switch (tema) {
                case "cp-registro":
                    procesarRegistro(cpId, mensaje);
                    break;
                case "cp-estado":
                    procesarActualizacionEstado(cpId, mensaje);
                    break;
                case "cp-autorizacion":
                    procesarAutorizacion(cpId, mensaje);
                    break;
                case "actualizacion-recarga":
                    procesarActualizacionRecarga(cpId, mensaje);
                    break;
                case "fallo-cp":
                    procesarAveria(cpId, mensaje);
                    break;
                case "recuperacion-cp":
                    procesarRecuperacion(cpId, mensaje);
                    break;
                case "monitor-registro":
                    procesarRegistroMonitor(cpId, mensaje);
                    break;
                case "ticket":
                    procesarTicket(cpId, mensaje);
                    break;
                case "driver-solicitud":
                    procesarSolicitudDriver(cpId, mensaje);
                    break;
                default:
                    System.out.println("Tema no reconocido: " + tema);
            }
        } catch (Exception e) {
            System.err.println("Error procesando el mensaje [" + tema + "]: " + e.getMessage());
        }
    }

    // --- Métodos individuales (mantienen tu lógica original) ---

    private void procesarSolicitudDriver(String driverId, String mensaje) {
        System.out.println("Solicitud recibida de driver: " + driverId);
        try {
            registrarEvento(null, "SOLICITUD_DRIVER", "Solicitud recibida del driver " + driverId + " -> " + mensaje);
        } catch (Exception e) {
            System.err.println("[DB] Error registrando solicitud driver: " + e.getMessage());
        }
    }

    private void procesarRegistroMonitor(String cpId, String mensaje) {
        System.out.println("Monitor registrado para CP: " + cpId);
        productor.send(new ProducerRecord<>("central-to-monitor", cpId, "Monitor_Registro_OK|" + cpId));
        registrarEvento(cpId, "REGISTRO_MONITOR", "Monitor registrado: " + mensaje);
    }

    private void procesarRecuperacion(String cpId, String mensaje) {
        if (mensaje.startsWith("Recuperacion_Reporte"))
            System.out.println("Reporte del monitor, recuperacion en CP: " + cpId);
        else
            System.out.println("Recuperacion en CP: " + cpId);

        productor.send(new ProducerRecord<>("central-to-cp", cpId, "Recuperacion_ACK|" + cpId));
        productor.send(new ProducerRecord<>("sistema-eventos", cpId, "Recuperacion|" + cpId));

        registrarEvento(cpId, "RECUPERACION", "CP recuperado: " + mensaje);
        actualizarEstadoCP(cpId, "ACTIVADO", true);
    }

    private void procesarAveria(String cpId, String mensaje) {
        if (mensaje.startsWith("Averia_Reporte"))
            System.out.println("Reporte del monitor, averia en CP: " + cpId);
        else
            System.out.println("Averia en CP: " + cpId);

        productor.send(new ProducerRecord<>("central-to-cp", cpId, "Averia_ACK|" + cpId));
        productor.send(new ProducerRecord<>("sistema-eventos", cpId, "Averia|" + cpId));

        registrarEvento(cpId, "AVERIA", "Avería detectada: " + mensaje);
        actualizarEstadoCP(cpId, "AVERIADO", false);
    }

    private void procesarActualizacionRecarga(String cpId, String mensaje) {
        String[] partes = mensaje.split("\\|");
        if (partes.length < 4) return;

        String consumo = partes[2];
        String importe = partes[3];

        System.out.printf("CP: %s | Consumo: %s kW | Importe: %s €%n", cpId, consumo, importe);

        productor.send(new ProducerRecord<>("central-to-cp", cpId, "Consumo_OK|" + cpId));

        registrarEvento(cpId, "CONSUMO_UPDATE",
                String.format("Actualización de consumo: %s kWh / %s €", consumo, importe));
    }

    private void procesarAutorizacion(String cpId, String mensaje) {
        System.out.println("Procesando autorización CP " + cpId + ": " + mensaje);

        String tipoEvento = mensaje.contains("Aceptada") ? "AUTORIZACION_OK" : "AUTORIZACION_DENEGADA";
        registrarEvento(cpId, tipoEvento, mensaje);

        String ack = mensaje.contains("Aceptada") ? "Autorización_OK_ACK|" + cpId
                : "Autorización_DEN_ACK|" + cpId;
        productor.send(new ProducerRecord<>("central-to-cp", cpId, ack));
    }

    private void procesarActualizacionEstado(String cpId, String mensaje) {
        String[] partes = mensaje.split("\\|");
        if (partes.length < 4) return;

        String estado = partes[2];
        String funciona = partes[3];

        System.out.println("Estado actualizado CP: " + cpId + ": " + estado + " - Funciona: " + funciona);

        productor.send(new ProducerRecord<>("central-to-cp", cpId, "Actualizacion_OK|" + cpId));
        registrarEvento(cpId, "ACTUALIZACION_ESTADO", "Nuevo estado: " + estado);

        boolean funcionaBool = "Ok".equalsIgnoreCase(funciona);
        actualizarEstadoCP(cpId, estado, funcionaBool);
    }

    private void procesarRegistro(String cpId, String mensaje) {
        String[] partes = mensaje.split("\\|");
        if (partes.length < 4) return;

        String ubicacion = partes[2];
        String precio = partes[3];

        System.out.println("CP registrado: " + cpId + " en " + ubicacion + " - Precio: " + precio);

        productor.send(new ProducerRecord<>("central-to-cp", cpId, "Registro_OK|" + cpId));
        registrarEvento(cpId, "REGISTRO_CP", "CP registrado en " + ubicacion);

        try (Connection conn = DBManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE charging_point SET registrado_central=TRUE, estado='ACTIVADO', ubicacion=?, precio_kwh=? WHERE id=?")) {
            ps.setString(1, ubicacion);
            ps.setDouble(2, Double.parseDouble(precio));
            ps.setString(3, cpId);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] Error actualizando CP: " + e.getMessage());
        }
    }

    private void procesarTicket(String cpId, String mensaje) {
        String[] partes = mensaje.split("\\|");
        if (partes.length < 4) return;

        String conductorId = partes[1];
        String importe = partes[2];
        String consumo = partes[3];

        System.out.printf("Ticket enviado a %s - %s: %s kW (%s €)%n", conductorId, cpId, consumo, importe);
        productor.send(new ProducerRecord<>("central-to-cp", cpId, "Ticket_ACK|" + cpId));

        registrarEvento(cpId, "CONFIRMACION",
                String.format("Ticket enviado a %s - Consumo %s kWh / %s €", conductorId, consumo, importe));
    }

    // --- Métodos auxiliares BD ---

    private void registrarEvento(String cpId, String tipo, String descripcion) {
        try (Connection conn = DBManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO event_log (cp_id, tipo_evento, descripcion) VALUES (?, ?, ?)")) {
            ps.setString(1, cpId);
            ps.setString(2, tipo);
            ps.setString(3, descripcion);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] Error registrando evento: " + e.getMessage());
        }
    }

    private void actualizarEstadoCP(String cpId, String estado, boolean funciona) {
        try (Connection conn = DBManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE charging_point SET estado=?, funciona=?, ultima_actualizacion=NOW() WHERE id=?")) {
            ps.setString(1, estado);
            ps.setBoolean(2, funciona);
            ps.setString(3, cpId);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] Error actualizando estado CP: " + e.getMessage());
        }
    }

    public void detener() {
        ejecucion = false;
        this.interrupt();
        System.out.println("Hilo servidor detenido");
    }
}
