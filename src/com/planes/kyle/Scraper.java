package com.planes.kyle;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.Statement;

import static com.planes.kyle.Parser.conn;

public class Scraper
{
    public static String getRoute(String ICAO)
    {
        String registration = "";
        try {
            String query = "SELECT registration FROM Aircrafts WHERE icao24 ='"+ICAO+"'";
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(query);
            if(rs.next()) {
                registration = rs.getString("registration");
            }
            st.close();
            if(registration == null || registration == "") {
                return "N/A-N/A";
            }
            final String webSiteURL = "https://flightaware.com/live/flight/" + registration;
            //Connect to the website and get the html
            Document doc = Jsoup.connect(webSiteURL).get();
            String htmlDoc = (doc.outerHtml());
            String shortenedDoc = doc.title();
            //int hi = htmlDoc.indexOf("flightPageBlockedData");

            if(shortenedDoc.contains(registration)) {
                int result1 = (htmlDoc.indexOf("origin\" content=", 15350));
                String originAirport = "";
                originAirport += htmlDoc.charAt(result1 + 18);
                originAirport += htmlDoc.charAt(result1 + 19);
                originAirport += htmlDoc.charAt(result1 + 20);
                //System.out.println(originAirport);


                int result2 = (htmlDoc.indexOf("destination\" content=", 15000));
                String destAirport = "";
                destAirport += htmlDoc.charAt(result2 + 23);
                destAirport += htmlDoc.charAt(result2 + 24);
                destAirport += htmlDoc.charAt(result2 + 25);
                //System.out.println(destAirport);
                return originAirport + "-" + destAirport;
            }
            else{
                return "N/A-N/A";
            }
        }

        catch (Exception e){
            e.printStackTrace();
            Email.sendErrorEmail(e);
            return "problem";
        }
    }
    public static boolean getPhoto(String registration, String path){
        try {
            //final String webSiteURL = "https://www.jetphotos.com/photo/keyword/" + registration;
            final String webSiteURL = "https://www.airplane-pictures.net/search.php?p="+registration+"&Submit=Search";
            //Connect to the website and get the html
            Document doc = Jsoup.connect(webSiteURL).referrer("https://www.google.com").get();
            String htmlDoc = (doc.outerHtml());
            //System.out.print(htmlDoc);

            String link = "//cdn.airplane-pictures.net";
            String webLink = "";
            int result2 = (htmlDoc.indexOf("//cdn.airplane-pictures.net",10000));
            if(result2 == -1){
                return false; //did not find anything for this plane
            }

            int i = link.length();
            //link += "/full";
            while(true){
                if(htmlDoc.charAt(i+result2) != '"'){
                    link += htmlDoc.charAt(i+result2);
                }
                else{
                    link = link.replaceAll("as", "");
                    webLink = "https:" + link;
                    break;
                }
                i++;
            }
            if(saveImage(webLink,path)){
                return true;
            }
            else{
                return false;
            }

        }
        catch (Exception e){
            e.printStackTrace();
            Email.sendErrorEmail(e);
            return false;
        }
    }

    private static boolean saveImage(String imageUrl, String path) throws IOException {
        BufferedImage image =null;
        try{
            URL url =new URL(imageUrl);
            //System.out.println(imageUrl.equals("https://cdn.airplane-pictures.net/images/uploaded-images/2019/11/22/1254579.jpg"));
            // read the url
            image = ImageIO.read(url);

            // for png
            //ImageIO.write(image, "png",new File("/tmp/have_a_question.png"));

            // for jpg
            String fullPath = path + "/image.jpg";
            ImageIO.write(image, "jpg",new File(fullPath));
            return true;
        }
        catch(IOException e){
            System.out.println("Hi " + imageUrl);
            e.printStackTrace();
            Email.sendErrorEmail(e);
            return false;
        }
    }
}
