package task1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

@SuppressWarnings("rawtypes")
public class CompositeKey implements WritableComparable {
	private Long dateFrom;
	private Integer delay;
	
	public CompositeKey(){}
	
	public CompositeKey(long dateFrom, String delay){
		this.setDateFrom(dateFrom);
		if("NA".equals(delay)) 
			this.delay=Integer.MIN_VALUE;
		else if("#".equals(delay))
			this.delay = Integer.MAX_VALUE;
		else
			this.delay= Integer.valueOf(delay);
	}
	
	public CompositeKey(String stringKey){
		if(stringKey.contains(",")){
			String[] k = stringKey.split(",");
			this.dateFrom = Long.valueOf(k[0]);
			this.delay = Integer.valueOf(k[1]);
		}
	}

	@Override
	public String toString(){
		return (new StringBuilder()).append(dateFrom).append(",").append(delay).toString();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.dateFrom);
		out.writeInt(this.delay);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.dateFrom = in.readLong(); 
		this.delay = in.readInt(); 		
	}
	
	@Override
	public int compareTo(Object o) {
		int result = dateFrom.compareTo(((CompositeKey)o).dateFrom);
		if(0==result){
			// for desc order on ArrDelay
			result = -1*delay.compareTo(((CompositeKey)o).delay);
		}
		return result;
	}

	public Long getDateFrom() {
		return dateFrom;
	}

	public void setDateFrom(Long dateFrom) {
		this.dateFrom = dateFrom;
	}


	public Integer getDelay() {
		return delay;
	}

	public void setDelay(Integer delay) {
		this.delay = delay;
	}
	
}