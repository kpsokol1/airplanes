package com.planes.kyle;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class AirlineIdentify
{
    public static void fixAirlines() throws SQLException {
        String airlineName = "No Info";
        String callsign = "";
        String ICAO = "";
        int id = -1;
        int i = 0;
        String query4 = "";

        String query0 = "SELECT ICAO,Callsign,id FROM Raw1 limit 9999999999 offset 15500000";
        Statement st1 = Parser.conn.createStatement();
        Statement st2 = Parser.conn.createStatement();
        Statement st3 = Parser.conn.createStatement();
        Statement st4 = Parser.conn.createStatement();
        Statement st5 = Parser.conn.createStatement();

            ResultSet rs1 = st1.executeQuery(query0);

            while(rs1.next()){
                airlineName = "No Info";
                callsign = rs1.getString("Callsign");
                id = rs1.getInt("id");
                if(callsign == null){
                    callsign = "";
                }
                ICAO = rs1.getString("ICAO");
                String query1 = "SELECT Name FROM Airline_Codes WHERE ICAO_Code ='" + callsign.replaceAll("\\d.*", "") + "'";
                ResultSet rs2 = st2.executeQuery(query1);
                if(rs2.next() && !ICAO.equals("ad3968")){
                    airlineName = rs2.getString("Name");
                }
                else{
                    String query2 = "SELECT operatoricao FROM Aircrafts WHERE icao24 ='" + ICAO + "'";
                    ResultSet rs3 = st3.executeQuery(query2);
                    if(rs3.next()){
                        callsign = rs3.getString("operatoricao");
                        String query3 = "SELECT Name FROM Airline_Codes WHERE ICAO_Code ='" + callsign + "'";
                        ResultSet rs4 = st4.executeQuery(query3);
                        if(rs4.next()) {
                            airlineName = rs4.getString("Name");
                        }
                    }
                }
                System.out.println(id);
                query4 = "Update Raw1 Set Airline = '" + airlineName + "' where ICAO = '" + ICAO + "' and id = " + id + " ";
                st5.addBatch(query4);
                if(i % 100 == 0){
                    st5.executeBatch();
                    st5.clearBatch();
                }
                i++;
            }
            st5.executeBatch();
            st1.close();
            st2.close();
            st3.close();
            st4.close();
            st5.close();
        }
    public static final String [] emailList = {"BAW", "AFR", "KLM", "UAE", "QFA", "DLH", "SIA", "QTR","KAL"};
    public static final String [] emailTailNumbersList = {"N778AN", "N756US", "N915FJ", "N120EE"};
    public static final String [] emailPlanesList = { "A380", "A350", "A220", "A340", "AN-124", "An-225", "Dreamlifter", "Beluga"};
}
