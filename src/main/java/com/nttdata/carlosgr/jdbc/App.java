package com.nttdata.carlosgr.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Clase principal de la aplicación.
 * 
 * @author Carlos González Ruiz - NTT Data
 */
public class App {
	/** LOGGER */
	private static final Logger LOG = LoggerFactory.getLogger(App.class);

	/** Dirección a la base de datos. */
	private static final String DB_ADDRESS = "jdbc:mysql://localhost:3306/nttdata_jdbc_ex";
	/** Usuario a la base de datos. */
	private static final String DB_USER = "root"; /* carlosAdmin */
	/** Contraseña a la base de datos. */
	private static final String DB_PASSWORD = "rootroot"; /* 1234567890 */

	/**
	 * Método "Main" de la clase.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		try {

			String query = """
					SELECT sp.name AS "Nombre", 
					       st.name AS "Equipo",
					       sp.first_rol AS "Demarcación",
					       sp.second_rol AS "Demarcación alternativa",
					       sp.birth_date AS "Fecha de nacimiento"
					FROM nttdata_mysql_soccer_player sp
					JOIN nttdata_mysql_soccer_team st ON sp.id_soccer_team = st.id_soccer_team""";

			// Mostrar consulta como si se ejecutase como una órden por consola.
			LOG.info("\n> {}\n\n", query);

			// Realizar y mostrarconsulta.
			String queryOutput = getStringFromQuery(query);
			LOG.info("\n{}", queryOutput);

		} catch (ClassNotFoundException | SQLException e) {
			LOG.error("No se pudo realizar las operaciones a la base de datos: {}", e.getMessage());
		}
	}

	/**
	 * Método que devuelve un ResultSet de Establece la conexión con base de datos
	 * MySQL.
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @param query
	 * @return String
	 */
	private static String getStringFromQuery(String query) throws ClassNotFoundException, SQLException {
		String out = "";

		// Conexión a la base de datos.
		LOG.debug("Realizando conexión a la base de datos...");
		Connection connection = setConnection();
		LOG.debug("Se ha podido realizar la conexión.");

		try (Statement sentence = connection.createStatement()) {
			// Realizar la query.
			ResultSet resultQuery = sentence.executeQuery(query);

			// Tratamiento de datos.
			if (resultQuery != null) {

				// Obtener información sobre las columnas.
				ResultSetMetaData resultQueryMetadata = resultQuery.getMetaData();

				StringBuilder playerString = new StringBuilder();
				while (resultQuery.next()) {

					// Iterar sobre las columnas de lo que devuelve la consulta.
					for (int i = 1; i <= resultQueryMetadata.getColumnCount(); ++i) {
						playerString.append(resultQueryMetadata.getColumnLabel(i));
						playerString.append(": ");
						playerString.append(resultQuery.getString(resultQueryMetadata.getColumnLabel(i)));
						playerString.append(" | ");
					}

					playerString.append("\n");

				}

				out = playerString.toString();

			}
		} catch (SQLException e) {
			LOG.error("No se pudo definir \"sentence\": {}", e.getMessage());
		}

		// Cerrar conexión.
		connection.close();
		LOG.debug("Conexión a la base de datos cerrada.");

		return out;
	}

	/**
	 * Método que realiza una conexión a una base de datos y devuelve el objeto de
	 * conexión. Los parámetros para realizar la conexión se obtienen de los
	 * atributos
	 * 
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 * @return Connection
	 */
	private static Connection setConnection() throws SQLException, ClassNotFoundException {
		// Establecer driver de conexión.
		Class.forName("com.mysql.cj.jdbc.Driver");

		// Devolver objeto de conexión de base de datos.
		return DriverManager.getConnection(DB_ADDRESS, DB_USER, DB_PASSWORD);
	}

}
