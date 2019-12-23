package lux_datapump.lux_datapump;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Performer {
	
	
	public List<Integer> perform(Connection connection/* = getDatabaseConnection();*/, String query, List<List<Object>> data) throws SQLException {
		
//        connection.setAutoCommit(true);
		
        ;

		
		try(PreparedStatement statement = connection.prepareStatement(query)) {
			for (List<Object> oo: data) {
				int i = 1;
				// TODO
				for (Object o : oo) {
					statement.setObject(i++, o);
				}
				statement.addBatch();
			};
			int[] executeBatch = statement.executeBatch();
			return new AbstractList<Integer>() {

				@Override
				public Integer get(int index) {
					return Integer.valueOf(executeBatch[index]);
				}

				@Override
				public int size() {
					return executeBatch.length;
				}
			};
		} finally {
			
		} 
		
	}

}
