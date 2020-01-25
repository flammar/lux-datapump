package lux_datapump.lux_datapump;

public class PortionedVolatileCounterImpl implements Counter {
	public final int portion;
	private int accessedTimes;
	private volatile long count = 0;
	private long countLastRead = 0;
	private Thread writer = null;

	private long countReal = 0;

	public PortionedVolatileCounterImpl(int portion) {
		this.portion = portion;
		accessedTimes = this.portion;
	}

	public PortionedVolatileCounterImpl() {
		this(100);
	}

	@Override
	public void set(long v) {
		countReal = count = countReal = v;
	}

	@Override
	public void increment() {
		++countReal;
		if (writer == null)
			writer = Thread.currentThread(); // Only one thread must increment
		if (mustFlush())
			count = countReal;
	}

	@Override
	public long get() {
		return (writer != null && writer == Thread.currentThread()) ? countReal : mustFlush() ? (countLastRead = count)
				: countLastRead < countReal ? (countLastRead = countReal) : countLastRead;
	}

	private boolean mustFlush() {
		final boolean must = --accessedTimes < 0;
		if (must)
			accessedTimes = portion;
		return must;
	}

}
