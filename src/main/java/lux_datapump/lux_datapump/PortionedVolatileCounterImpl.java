package lux_datapump.lux_datapump;

public class PortionedVolatileCounterImpl implements Counter{
	public final int portion;
	private volatile long count =0;
	private long countLast =0;
	private long countReal =0;

	private PortionedVolatileCounterImpl(int portion) {
		this.portion = (portion + 1) >> 1;
	}

	@Override
	public void set(long v) {
		countReal = count = countReal = v;
	}

	@Override
	public void increment() {
		if(++countReal - countLast >= portion) count = countLast = countReal;
	}

	@Override
	public long get() {
		return count;
	}

}
