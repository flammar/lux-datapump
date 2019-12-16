package lux_datapump.lux_datapump;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Performer {
	
	public int[] perform(Connection connection/* = getDatabaseConnection();*/, String query, Object[][] data) throws SQLException {
		
//        connection.setAutoCommit(true);
		
        ;

		
		try(PreparedStatement statement = connection.prepareStatement(query)) {
			for (int index = 0; index < data.length; index++) {
				Object[] objects = data[index];
				for (int j = 0; j < objects.length; j++) {
					statement.setObject(j + 1, objects[j]);
				}
				statement.addBatch();
			}
			return statement.executeBatch();
		} finally {
			
		} 
		
	}

}
