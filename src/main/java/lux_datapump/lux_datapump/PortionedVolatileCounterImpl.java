package lux_datapump.lux_datapump;

public class PortionedVolatileCounterImpl implements Counter {
	public final int portion;
	private int portionRead;
	private int portionWritten;
	private volatile long count = 0;
//	private long countLastWritten = 0;
	private long countLastRead = 0;
	private Thread writer = null;

	private long countReal = 0;

	private PortionedVolatileCounterImpl(int portion) {
		this.portion = portion;
		// (portion + 1) >> 1;
		portionRead = this.portion;
	}

	@Override
	public void set(long v) {
		countReal = count = countReal = v;
	}

	@Override
	public void increment() {
//		if (++countReal - countLastWritten >= portion)
//			count = countLastWritten = countReal;
		++countReal;
		if(writer == null )writer = Thread.currentThread();
		if ( portionWritten-- <= 0) {
			portionWritten = portion;
			count = countReal;
		}
	}

	@Override
	public long get() {
		if (portionRead-- > 0)
			return countLastRead;
		else {
			portionRead = portion;
			return countLastRead = count;
		}
	}

}
