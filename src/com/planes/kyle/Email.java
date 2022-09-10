package com.planes.kyle;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.time.LocalDateTime;
import java.util.*;
import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.*;
import javax.mail.internet.*;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Email {
    //keeps from making copies of the instance variables evrytime the function is called
    private static final String emailUsername = Utilities.getAuth("emailUsername");
    private static final String emailPassword = Utilities.getAuth("emailPassword");


    public static void sendEmail(String sendTo, String subjectLine, String messageToSend, String fileLocation) {

        try {
            // Get system properties
            Properties properties = System.getProperties();

            //Setup mail server
            properties.setProperty("mail.smtp.auth", "true");

            //Use secure connection (SSL/TLS)
            properties.setProperty("mail.smtp.starttls.enable", "true");

            //Set SMTP server hostname
            properties.setProperty("mail.smtp.host", "smtp.gmail.com");

            //Need to trust certificate from server
            properties.setProperty("mail.smtp.ssl.trust", "smtp.gmail.com");

            //Set port
            properties.setProperty("mail.smtp.port", "587");


            //Set username and password for sending mail account
            properties.setProperty("mail.smtp.user", emailUsername);
            properties.setProperty("mail.smtp.password", emailPassword);

            //Get the default session object.
            Session session = Session.getInstance(properties, new Authenticator()
            {
                @Override
                protected PasswordAuthentication getPasswordAuthentication()
                {
                    return new PasswordAuthentication(emailUsername, emailPassword);
                }
            });

            //Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);

            //Set From: header field of the header.
            message.setFrom(new InternetAddress(emailUsername));

            //Set To: header field of the header.
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(sendTo));

            //Set Subject: header field
            message.setSubject(subjectLine);

            //Create the message part
            BodyPart messageBodyPart = new MimeBodyPart();

            //Set the body of the email
            messageBodyPart.setText(messageToSend);
            //Create a multipart message
            Multipart multipart = new MimeMultipart();

            //Set text message part
            multipart.addBodyPart(messageBodyPart);

            //Attach file
            if(fileLocation != "none") {
                messageBodyPart = new MimeBodyPart();
                DataSource source = new FileDataSource(fileLocation);
                messageBodyPart.setDataHandler(new DataHandler(source));
                messageBodyPart.setFileName(fileLocation);
                multipart.addBodyPart(messageBodyPart);
            }


            //Send the complete message parts
            message.setContent(multipart);

            //Send message
            Transport.send(message);
            System.out.println("Message Sent");
            //lastTimeEmailSent = getCurrentTime().toString();
        }
        catch (MessagingException ex) {
            ex.printStackTrace();
            System.out.println(LocalDateTime.now());
            System.out.println("Email1");
        }
    }

    public static void sendPlaneAlert(String subjectLine, String messageToSend, String fileLocation, String stationName, String user_id)
        throws SQLException {
        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
        formatter.setTimeZone(TimeZone.getDefault());
        Date date = new Date();

        String query = "SELECT email,lou_alerts, ala_alerts FROM Users WHERE user_id ='" + user_id + "'";
        Statement st1 = Parser.conn.createStatement();
        ResultSet rs1 = st1.executeQuery(query);
        boolean lou_alerts = false;
        boolean ala_alerts = false;
        String email = "";

        while(rs1.next()){
            email = rs1.getString("email");
            lou_alerts = (rs1.getInt("lou_alerts") == 1);
            ala_alerts = (rs1.getInt("ala_alerts") == 1);
            if(stationName.equals("Louisville,KY")){
                if (lou_alerts){
                    sendEmail(email,subjectLine,messageToSend + "\n" + "http://10.128.107.117/combine1090" + "\nTime Sent: " + formatter.format(date) + " " + TimeZone.getDefault().getDisplayName(true,TimeZone.SHORT),fileLocation);
                }
            }
            else if(stationName.equals("Tuscaloosa,AL")){
                if (ala_alerts){
                    sendEmail(email,subjectLine,messageToSend + "\n" + "http://10.128.107.117/combine1090" + "\nTime Sent: " + formatter.format(date) + " " + TimeZone.getDefault().getDisplayName(true,TimeZone.SHORT),fileLocation);
                }
            }
        }
        st1.close();
    }

    public static void sendErrorEmail(Exception e){
        String query = "SELECT error_email, error_alerts from Users where error_alerts = 1";

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);

        SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a");
        formatter.setTimeZone(TimeZone.getDefault());
        Date date = new Date();

        try{
            Statement st1 = Parser.conn.createStatement();
            ResultSet rs1 = st1.executeQuery(query);
            while(rs1.next()){
                sendEmail(rs1.getString("error_email"), "Error Encountered on Radar24",sw.toString() + "\nTime Sent: " + formatter.format(date) + " " + TimeZone.getDefault().getDisplayName(true,TimeZone.SHORT),"none");
            }
        }
        catch(SQLException e1){
            e1.printStackTrace();
            System.out.println(LocalDateTime.now());
            System.out.println("Email2");
        }
    }
}



