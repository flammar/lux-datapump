package lux_datapump.lux_datapump;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ColumnDescriptor {
	public enum Getter {
		GET_OBJECT {
			@Override
			public Object perform(ResultSet rs, int index) throws SQLException {
				return rs.getObject(index + 1);
			}
		};

		public abstract Object perform(ResultSet rs, int index) throws SQLException;
	}

	public enum Setter {
		SET_OBJECT_OR_NULL {

			@Override
			public void perform(PreparedStatement ps, int index, Object data, int sqlType) throws SQLException {
				if (data == null)
					ps.setNull(index + 1, sqlType);
				else
					ps.setObject(index + 1, data, sqlType);
			}
		};

		public abstract void perform(PreparedStatement ps, int index, Object data, int sqlType) throws SQLException;
	}

	public final String dstName;
	public final String dstExpr;
	public final String srcExpr;
	public final int sqlType;
	public final Setter setter;
	public final Getter getter;
	public final int srcIndex;
	public final int dstIndex;

	public ColumnDescriptor(int index, final String name, final int sqlType) {
		super();
		this.srcExpr = this.dstName = name;
		this.sqlType = sqlType;
		dstExpr = "?";
		this.getter = Getter.GET_OBJECT;
		this.setter = Setter.SET_OBJECT_OR_NULL;
		this.srcIndex = index;
		this.dstIndex = index;
	}
	
	public Object get(ResultSet rs) throws SQLException {
		return rs.getObject(srcIndex);
	}
	
	public void set(PreparedStatement rs, Object val) throws SQLException {
		rs.setObject(dstIndex, val, sqlType);
	}
}