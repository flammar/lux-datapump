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
		},
		GET_STRING {
			@Override
			public Object perform(ResultSet rs, int index) throws SQLException {
				return rs.getString(index);
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
	public ColumnDescriptor(String srcExpr, int sqlType, String dstName, String dstExpr, Getter getter, Setter setter,
			int srcIndex, int dstIndex) {
		super();
		this.dstName = dstName;
		this.dstExpr = dstExpr;
		this.srcExpr = srcExpr;
		this.sqlType = sqlType;
		this.setter = setter;
		this.getter = getter;
		this.srcIndex = srcIndex;
		this.dstIndex = dstIndex;
	}

	public final Setter setter;
	public final Getter getter;
	public final int srcIndex;
	public final int dstIndex;

	public ColumnDescriptor(int index, final String name, final int sqlType) {
		this(name, sqlType, name, "?", Getter.GET_OBJECT, Setter.SET_OBJECT_OR_NULL, index, index);
//		this.srcExpr = this.dstName = name;
//		this.sqlType = sqlType;
//		dstExpr = "?";
//		this.getter = Getter.GET_OBJECT;
//		this.setter = Setter.SET_OBJECT_OR_NULL;
//		this.srcIndex = index;
//		this.dstIndex = index;
	}
}