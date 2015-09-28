package task1;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	
	protected CompositeKeyComparator(){
		super(CompositeKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b){
		CompositeKey key1 = (CompositeKey) a;
		CompositeKey key2 = (CompositeKey) b;
		
		int compare =  key1.getDateFrom().compareTo(key2.getDateFrom());
		if(compare==0) {
			compare = -1*key1.getDelay().compareTo(key2.getDelay());
			}
		
		return compare;
	}

}
