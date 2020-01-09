package lux_datapump.lux_datapump;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ValueDescriptor {
	public final ColumnDescriptor descriptor;
	public final Object value;

	public ValueDescriptor(Object value, ColumnDescriptor descriptor) {
		super();
		this.value = value;
		this.descriptor = descriptor;
	}
	
	public void set(PreparedStatement preparedStatement) throws SQLException {
		descriptor.set(preparedStatement, value);
	}

	public static ValueDescriptor create(ResultSet resultSet, ColumnDescriptor descriptor) throws SQLException {
		return new ValueDescriptor(descriptor.get(resultSet), descriptor);
	}
	
	

}
