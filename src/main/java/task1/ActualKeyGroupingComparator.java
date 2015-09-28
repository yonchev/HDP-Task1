package task1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class ActualKeyGroupingComparator extends WritableComparator {
	
	protected ActualKeyGroupingComparator() {
		super(CompositeKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b){
		CompositeKey key1 = (CompositeKey) a;
		CompositeKey key2 = (CompositeKey) b;
		return key1.getDateFrom().compareTo(key2.getDateFrom());
	}
}
