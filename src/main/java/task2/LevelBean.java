package task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LevelBean implements Writable{

    public void setStationNum(int StationNum){
        this.StationNum = StationNum;
    }
    public int getStationNum(){return StationNum;}
    public void setDay(int day){
        this.day= day;
    }
    public int getDay(){return day;}
    public void setHour(int hour){
        this.hour = hour;
    }
    public int getHour(){return hour;}
    public void setAQI(int AQI){this.AQI = AQI;}
    public int getAQI(){return AQI;}

    public LevelBean(){}
    public LevelBean(int StationNum, int day, int hour, int AQI){
        super();
        setStationNum(StationNum);
        setDay(day);
        setAQI(AQI);
    }

    private int StationNum;
    private int day;
    private int hour;
    private int AQI;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(StationNum);
        dataOutput.writeInt(day);
        dataOutput.writeInt(hour);
        dataOutput.writeInt(AQI);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        StationNum = dataInput.readInt();
        day = dataInput.readInt();
        hour = dataInput.readInt();
        AQI = dataInput.readInt();
    }


}
