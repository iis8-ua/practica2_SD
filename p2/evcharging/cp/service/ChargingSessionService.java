package p2.evcharging.cp.service;

import p2.db.DBManager;
import p2.evcharging.cp.ChargingPoint;
import p2.evcharging.cp.CPState;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class ChargingSessionService {
    private ChargingPoint cp;
    private String sessionId;
    private String conductorId;
    private String tipo; // Manual o Automatico
    private boolean enCurso;
    private double energiaTotal;
    private double importeTotal;

    public ChargingSessionService(ChargingPoint cp) {
        this.cp = cp;
        this.enCurso = false;
        this.energiaTotal = 0.0;
        this.importeTotal = 0.0;
    }

    /**
     * Inicia una sesión de carga y la registra en la base de datos.
     */
    public boolean iniciarSuministro(String conductorId, String tipo) {
        if (enCurso) {
            System.err.println("[SESSION] Ya hay una sesión activa en " + cp.getId());
            return false;
        }

        try {
            this.sessionId = generarSessionId(cp.getId());
            this.conductorId = conductorId;
            this.tipo = tipo;
            this.enCurso = true;
            this.cp.setConductorActual(conductorId);
            this.cp.setEstado(CPState.SUMINISTRANDO);
            this.cp.actualizarEstadoBD();

            this.energiaTotal = 0.0;
            this.importeTotal = 0.0;

            try (Connection conn = DBManager.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "INSERT INTO charging_session (session_id, cp_id, conductor_id, tipo, estado, energia_total, importe_total) " +
                                 "VALUES (?, ?, ?, ?, 'EN_CURSO', 0, 0)")) {
                ps.setString(1, sessionId);
                ps.setString(2, cp.getId());
                ps.setString(3, conductorId);
                ps.setString(4, tipo);
                ps.executeUpdate();
                System.out.println("[DB] Sesión iniciada: " + sessionId);
            }

            return true;
        } catch (Exception e) {
            System.err.println("[SESSION] Error iniciando suministro: " + e.getMessage());
            return false;
        }
    }

    /**
     * Actualiza el consumo e inserta una nueva fila en charging_update.
     */
    public void registrarConsumo(double kw) {
        if (!enCurso) return;

        this.energiaTotal += kw;
        this.importeTotal = this.energiaTotal * cp.getPrecioKwh();
        cp.actualizarConsumo(kw);

        try (Connection conn = DBManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "INSERT INTO charging_update (session_id, cp_id, consumo, importe) VALUES (?, ?, ?, ?)")) {
            ps.setString(1, sessionId);
            ps.setString(2, cp.getId());
            ps.setDouble(3, energiaTotal);
            ps.setDouble(4, importeTotal);
            ps.executeUpdate();
            System.out.printf("[DB] Actualización consumo CP %s -> %.2f kWh, %.2f €%n", cp.getId(), energiaTotal, importeTotal);
        } catch (SQLException e) {
            System.err.println("[DB] Error registrando actualización de consumo: " + e.getMessage());
        }

        try (Connection conn = DBManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE charging_session SET energia_total=?, importe_total=? WHERE session_id=?")) {
            ps.setDouble(1, energiaTotal);
            ps.setDouble(2, importeTotal);
            ps.setString(3, sessionId);
            ps.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] Error actualizando totales en sesión: " + e.getMessage());
        }
    }

    /**
     * Finaliza la sesión y actualiza la BD.
     */
    public void finalizarSuministro() {
        if (!enCurso) {
            System.err.println("[SESSION] No hay sesión activa para finalizar.");
            return;
        }

        try (Connection conn = DBManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE charging_session SET estado='FINALIZADA', fin=NOW(), energia_total=?, importe_total=? WHERE session_id=?")) {
            ps.setDouble(1, energiaTotal);
            ps.setDouble(2, importeTotal);
            ps.setString(3, sessionId);
            ps.executeUpdate();

            System.out.printf("[DB] Sesión finalizada %s: %.2f kWh, %.2f €%n", sessionId, energiaTotal, importeTotal);
        } catch (SQLException e) {
            System.err.println("[DB] Error finalizando sesión: " + e.getMessage());
        }

        // Limpieza local
        this.enCurso = false;
        this.cp.setEstado(CPState.PARADO);
        this.cp.setConductorActual(null);
        this.cp.actualizarEstadoBD();
        this.sessionId = null;
    }

    private String generarSessionId(String cpId) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        return "SES_" + cpId + "_" + LocalDateTime.now().format(formatter);
    }

    // Getters
    public boolean isEnCurso() { return enCurso; }
    public double getEnergiaTotal() { return energiaTotal; }
    public double getImporteTotal() { return importeTotal; }
    public String getSessionId() { return sessionId; }
}
