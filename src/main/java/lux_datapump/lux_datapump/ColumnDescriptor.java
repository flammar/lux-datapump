package lux_datapump.lux_datapump;

public class ColumnDescriptor {
	public final String dstName;
	public final String dstExpr;
	public final String srcExpr;
	public final int sqlType;

	public ColumnDescriptor(final String name, final int sqlType) {
		super();
		this.srcExpr = this.dstName = name;
		this.sqlType = sqlType;
		dstExpr = "?";
	}
}