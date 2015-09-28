package task1;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaskMapper extends Mapper<Object, Text, CompositeKey, Text> {
	
	private Text outvalue = new Text();
	

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		DateFormat frm = new SimpleDateFormat("yyyy-MM-dd");
		String[] fields = value.toString().split(",");
		
		//map from flightdelays and skip first title row
		if(fields.length > 7 && !fields[0].equals("Year") && "SFO".equals(fields[17])){
			String year = fields[0];
			String month = fields[1];
			String dayByMonth = fields[2];
			String delay = fields[15];
			StringBuilder sb = new StringBuilder();
			sb.append(year).append("-").append(month).append("-").append(dayByMonth);
			Date dateKey = null;
			
			try {
				dateKey = (Date) frm.parse(sb.toString());
			} catch (ParseException ignored) {}
			CompositeKey compositeKey = new CompositeKey(dateKey.getTime(),delay);

			// will take only needed fields
			// Year,Month,DayofMonth,DepTime,ArrTime,UniqueCarrier,FlightNum,ActualElapsedTime,ArrDelay,DepDelay,Origin,Dest
			String[] arr = {fields[0],fields[1],fields[2],fields[4],fields[6],fields[8],fields[9],fields[11],fields[14],fields[15],fields[16],fields[17]};

			// convert into string separated by ','
			String val = Arrays.toString(arr);
			val = val.substring(1,val.length()-1);
			
			outvalue.set(val);
			context.write(compositeKey, outvalue);
		}
		//map from weather and skip first title row
		else if(fields.length==7 && !fields[0].equals("STATION_NAME")){
				String year = fields[1];
				String month = fields[2];
				String dayByMonth = fields[3];
				
				StringBuilder sb = new StringBuilder();
				sb.append(year).append("-").append(month).append("-").append(dayByMonth);
				Date dateKey = null;
				try {
					dateKey = (Date) frm.parse(sb.toString());
				} catch (ParseException ignored) {}
				
				// will take only needed fields
				// PRCP,TMAX,TMIN
				String[] arr = {fields[4],fields[5],fields[6]};

				String val = Arrays.toString(arr);
				val = val.substring(1,val.length()-1);
				
				CompositeKey compositeKey = new CompositeKey(dateKey.getTime(),"#"); // set MAX_INT, look into constructor
				
				outvalue.set("B"+val); // we will add char B to match from him in reducer
				context.write(compositeKey, outvalue);
			
		}
		
	}

}
