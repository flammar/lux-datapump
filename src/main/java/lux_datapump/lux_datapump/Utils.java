package lux_datapump.lux_datapump;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Utils {

	private Utils() {
	}

	static Iterator<List<Object>> toIterator(ResultSet src, int size) {
			return new Iterator<List<Object>>() {
	//			private final int size = ;
				private boolean wasNext = false, wentOut = false;
				private List<Object> current = null; 
	
				@Override
				public boolean hasNext() {
					return tryNext(src);
				}
	
				@Override
				public List<Object> next() {
					if(! hasNext()) return null;
					return current;
				}
	
				private boolean tryNext(ResultSet src) {
					if(!wentOut && !wasNext) {
						try {
							wentOut = !src.next();
							current = wentOut ? null : new ArrayList<>(rowToList(src));
							wasNext = true;
						} catch (SQLException e) {
							wentOut=true;
							current = null;
						}
					}
					return !wentOut;
				}
	
				private AbstractList<Object> rowToList(ResultSet src) {
					final ResultSet src1 = src;
					return new AbstractList<Object>() {
					
						
						@Override
						public Object get(final int index) {
							try {
								return src1.getObject(index + 1);
							} catch (final SQLException e) {
								e.printStackTrace();
							}
							return null;
						}
					
						@Override
						public int size() {
							return size;
						}
					};
				}
			};
		}

	static <T> Iterator<Iterable<T>> portionize(Iterator<T> baseIterator, int portionSize) {
		return new Iterator<Iterable<T>>() {
	
			@Override
			public boolean hasNext() {
				return baseIterator.hasNext();
			}
	
			@Override
			public Iterable<T> next() {
				return new Iterable<T>() {
					private int i=portionSize;
					private final Iterator<T> base = baseIterator;
					
					@Override
					public Iterator<T> iterator() {
						return new Iterator<T>() {
							
							@Override
							public T next() {
								i--;
								return base.hasNext() ? base.next() : null;
							}
							
							@Override
							public boolean hasNext() {
								return i >0 && base.hasNext();
							}
						};
					}
				};
			}
		};
	}
	
	

}
