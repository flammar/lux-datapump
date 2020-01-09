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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lux_datapump.lux_datapump.Limit.Type;

public class Performer {
	private Integer portionSize;
	private String srcTableName;
	private String dstTableName;
	/**
	 * [<lower> -> <upper>]
	 */
	private Map<String, Map.Entry<Limit, Limit>> limits;
	private String srcDataSource;
	private String dstDataSource;
	private List<ColumnDescriptor> dstColDescs;
	private boolean autocommit;
	private boolean predelete;

	public void perform() throws SQLException {
		String where = whereCond();

		try (final Connection dstConn = DriverManager.getConnection(dstDataSource);) {
			if (autocommit)
				dstConn.setAutoCommit(true);
			if (predelete)
				dstConn.createStatement().executeUpdate(String.format("DELETE FROM {0}{1}", dstTableName, where));
			dstColDescs = getColDescs(dstConn, dstTableName);
			final String dstColList = String.join(", ",
					dstColDescs.stream().map(d -> d.dstName).collect(Collectors.toList()));
			final List<String> dstValList = dstColDescs.stream().map(d -> d.dstExpr).collect(Collectors.toList());//Collections.nCopies(dstColDescs.size(), "?");
			final String dstQuery = MessageFormat.format("INSERT INTO {0} ( {1} ) VALUES ( {2} )", dstTableName,
					dstColList, String.join(", ", dstValList));
			final String srcColList = String.join(", ",
					dstColDescs.stream().map(d -> d.srcExpr).collect(Collectors.toList()));
			try (Connection srcConn = DriverManager.getConnection(srcDataSource);
					Statement srcStatement = srcConn.createStatement();
					ResultSet src = srcStatement.executeQuery(
							MessageFormat.format("SELECT {1} FROM {0}{2}", srcTableName, srcColList, where));) {
				Iterator<List<Object>> baseIterator = Utils.toIterator(src, dstColDescs.size());
				Utils.portionize(baseIterator, this.portionSize).forEachRemaining( data -> {
					try {
						perform(dstConn, dstQuery, data);
					} catch (SQLException e) {
						e.printStackTrace();
					}
				});
				
				
			} finally {

			}
		} finally {
		}
	}

	private String whereCond() {
		final String cond = limits.entrySet().stream()
				.flatMap(e -> Stream.of(new Limit(Type.LOWER, e.getKey()), new Limit(Type.UPPER, e.getValue())))
				.filter(l -> l.value != null).map(Objects::toString).collect(Collectors.joining(" AND "));
		return cond.length() > 0 ? " WHERE " + cond : "";
	}

	private List<ColumnDescriptor> getColDescs(final Connection dstConn, final String tableName) throws SQLException {
		try (Statement preStt = dstConn.createStatement();
				ResultSet executeQuery = preStt
						.executeQuery(MessageFormat.format("SELECT * FROM {0} WHERE 0 = 1", tableName));) {
			final ResultSetMetaData metaData = executeQuery.getMetaData();
			final ArrayList<ColumnDescriptor> arrayList = new ArrayList<ColumnDescriptor>(
					new AbstractList<ColumnDescriptor>() {
						int columnCount = metaData.getColumnCount();

						@Override
						public ColumnDescriptor get(final int index) {
							try {
								final int i = index + 1;
								return new ColumnDescriptor(i, metaData.getCatalogName(i),
										metaData.getColumnType(i));
							} catch (final SQLException e) {
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
		}
	}

	public List<Integer> perform(final Connection connection/* = getDatabaseConnection(); */, final String query,
			final Iterable<List<Object>> data) throws SQLException {
		try (PreparedStatement statement = connection.prepareStatement(query)) {
			for (final List<Object> oo : data) {
				int i = 1;
				for (final Object o : oo) {
					setObject(statement, i++, o);
				}
				statement.addBatch();
			}
			;
			final int[] executeBatch = statement.executeBatch();
			return new AbstractList<Integer>() {

				@Override
				public Integer get(final int index) {
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

	private void setObject(final PreparedStatement statement, final int i, final Object o) throws SQLException {
		statement.setObject(i, o, dstColDescs.get(i - 1).sqlType);
	}

}
