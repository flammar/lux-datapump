package lux_datapump.lux_datapump;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lux_datapump.lux_datapump.Performer.Limit.Type;

public class Performer {
	public static class Limit {
		public static enum Type {
			LOWER(true, ">"), UPPER(false, "<");

			public final boolean including;
			public final String expression;

			Type(boolean including, String expression) {
				this.including = including;
				this.expression = expression;

			}
		}

		public final Type type;
		public final Object value;
		public final boolean including;

		public Limit(Type type, Object value, boolean including) {
			this.type = type;
			this.value = value;
			this.including = including;
		}

		public Limit(Type type, Object value) {
			this(type, value, type.including);
		}

		public String conditionalExpression() {
			if (value == null)
				return null;
			return String.join(" ", "", type.expression + (including ? "=" : ""), "?", "");
		}
	}

	private Integer portionSize;
	private String srcTableName;
	private String dstTableName;
	private Map<String, Map.Entry<Limit, Limit>> limits;
	private String srcDataSource;
	private String dstDataSource;
	private List<ColumnDescriptor> dstColDescs;

	public static class ColumnDescriptor {
		public final String name;
		public final int sqlType;

		public ColumnDescriptor(String name, int sqlType) {
			super();
			this.name = name;
			this.sqlType = sqlType;
		}
	}

	public void perform() throws SQLException {
		String cond = limits.entrySet().stream()
				.flatMap(e -> Stream.of(new Limit(Type.LOWER, e.getKey()), new Limit(Type.UPPER, e.getValue())))
				.filter(l -> l.value != null).map(Objects::toString).collect(Collectors.joining(" AND "));
		Connection srcConn = DriverManager.getConnection(srcDataSource);
		Connection dstConn = DriverManager.getConnection(dstDataSource);
		dstColDescs = getColDescs(dstConn, dstTableName);
		String colList = String.join(", ", dstColDescs.stream().map(d -> d.name).collect(Collectors.toList()));
		String srcQuery = MessageFormat.format("SELECT {1} FROM {0}", srcTableName, colList);
		int size = dstColDescs.size();
		String dstQuery = MessageFormat.format("INSERT INTO {0} ( {1} ) VALUES ( {2} )", dstTableName, colList,
				String.join(", ", Collections.nCopies(size, "?")));

		try (Statement srcStatement = srcConn.createStatement(); ResultSet src = srcStatement.executeQuery(srcQuery);) {
			boolean wasNext;
			do {
				List<List<Object>> data = new ArrayList<>();
				for (int i = portionSize; (wasNext = src.next()) && i-- > 0;) {
					List<Object> oo0 = new AbstractList<Object>() {

						@Override
						public Object get(int index) {
							try {
								return src.getObject(index + 1);
							} catch (SQLException e) {
								e.printStackTrace();
							}
							return null;
						}

						@Override
						public int size() {
							return size;
						}
					};
					data.add(new ArrayList<>(oo0));
					perform(dstConn, dstQuery, data);
				}

			} while (wasNext);

		} finally {

		}
	}

	private List<ColumnDescriptor> getColDescs(Connection dstConn, String tableName) throws SQLException {
//		Statement preStt = dstConn.createStatement();

		try (Statement preStt = dstConn.createStatement();
				ResultSet executeQuery = preStt
						.executeQuery(MessageFormat.format("SELECT * FROM {0} WHERE 0 = 1", tableName));) {
			ResultSetMetaData metaData = executeQuery.getMetaData();
			ArrayList<ColumnDescriptor> arrayList = new ArrayList<Performer.ColumnDescriptor>(
					new AbstractList<ColumnDescriptor>() {
						int columnCount = metaData.getColumnCount();

						@Override
						public ColumnDescriptor get(int index) {
							try {
								return new ColumnDescriptor(metaData.getCatalogName(index + 1),
										metaData.getColumnType(index + 1));
							} catch (SQLException e) {
							}
							return null;
						}

						@Override
						public int size() {
							return columnCount;
						}
					});
			return arrayList;
		} finally {
			// TODO: handle finally clause
		}
	}

	public List<Integer> perform(Connection connection/* = getDatabaseConnection(); */, String query,
			List<List<Object>> data) throws SQLException {

//        connection.setAutoCommit(true);

		;

		try (PreparedStatement statement = connection.prepareStatement(query)) {
			for (List<Object> oo : data) {
				int i = 1;
				// TODO
				for (Object o : oo) {
					i = setObject(statement, i++, o);
				}
				statement.addBatch();
			}
			;
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

	private int setObject(PreparedStatement statement, int i, Object o) throws SQLException {
//		statement.setObject(i, o);
		statement.setObject(i, o, dstColDescs.get(i - 1).sqlType);

		return i;
	}

}
