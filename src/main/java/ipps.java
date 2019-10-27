/*
 * Chris Johnson
 * CS3810-001
 * 10/25/19
 * Database project 2
 */

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.*;
import java.util.*;


public class ipps {
    private static final String Configuration_File = "src/configuration.properties";

    // slice method like in python
    public static String slice_start(String s, int startIndex) {
        if (startIndex < 0) startIndex = s.length() + startIndex;
        return s.substring(startIndex);
    }
    // method to split string and not ignore comma provided by Mr. Motha.
    public static ArrayList<String> csv_split(String line) {
        ArrayList<String> data = new ArrayList<String>();
        boolean loop = true;
        while (loop) {
            String s = "";
            //String quote = String.valueOf('"');
            boolean inString = false;
            for (String c : line.split("")) {
                System.out.println(c);

                System.out.println(line);
                if (line.length() > 1) {
                    line = line.substring(1);
                }
                else {
                    loop = false;
                }

                //line = line.substring(1);
                if (c.equals(String.valueOf('"'))) {
                    inString = !inString;
                    continue;
                }
                if (!inString && c.equals(String.valueOf(','))) {
                    break;
                } else {
                    s += c;
                }
            }
            if (s.length() == 0) {
                break;
            }
            data.add(s);
        }
        return data;
    }

    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        prop.load(new FileInputStream(Configuration_File));

        String server = prop.getProperty("server");
        String database = prop.getProperty("database");
        String user = prop.getProperty("user");
        String password = prop.getProperty("password");
        String connectURL = "jdbc:mysql://" + server + "/" + database + "?serverTimezone=UTC&user=" + user + "&password=" + password;

        Connection conn = DriverManager.getConnection(connectURL);
        System.out.println("Connection to MySQL database " + database + " was successful!");


        // csv reader,  turns data into arrays and will need to load each array value before next line read
        String line = "";
        ArrayList<String> data = new ArrayList<String>();

        PreparedStatement preparedStmt = null;

        BufferedReader csvReader1 = new BufferedReader(new FileReader
                ("src\\input_files\\test1.csv" ));

        /*
        BufferedReader csvReader = new BufferedReader(new FileReader
                ("C:\\Users\\chris\\IdeaProjects\\ipps\\src\\input_files\\" +
                        "Inpatient_Prospective_Payment_System__IPPS__Provider_Summary_for_" +
                        "the_Top_100_Diagnosis-Related_Groups__DRG__-_FY2011.csv"));

        */
        // **************************************************************************

        try {
            // ***********************************************************************
            // insert test
            String query = " insert into ipps_main (DRG_Definition, Provider_ID, Provider_Name, " +
                    "Provider_Street_address, Provider_City, Provider_State, Provider_Zip, " +
                    "Hospital_Referral_Region, Total_Discharges, Average_Covered_Charges, " +
                    "Average_Total_Payments, Average_Medicare_Payments)"
                    + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            preparedStmt = conn.prepareStatement(query);

            String heading = csvReader1.readLine();
            while ((line = csvReader1.readLine()) != null) {
                data = csv_split(line);

                preparedStmt.setString (1, data.get(0).toString());
                preparedStmt.setInt    (2, Integer.parseInt(data.get(1)));
                preparedStmt.setString (3, data.get(2));
                preparedStmt.setString (4, data.get(3));
                preparedStmt.setString (5, data.get(4));
                preparedStmt.setString (6, data.get(5));
                preparedStmt.setInt    (7, Integer.parseInt(data.get(6)));
                preparedStmt.setString (8, data.get(7));
                preparedStmt.setInt    (9, Integer.parseInt(data.get(8)));
                preparedStmt.setFloat  (10, Float.parseFloat(data.get(9)));
                preparedStmt.setFloat  (11, Float.parseFloat(data.get(10)));
                preparedStmt.setFloat  (12, Float.parseFloat(data.get(11)));

                // execute the prepared statement
                preparedStmt.execute();

            }
            //test reader just 2 lines in csv file
            csvReader1.close();


            // **********************************************************
        }
        catch (SQLException ex){
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        finally {

            if (preparedStmt != null) {
                try {
                    preparedStmt.close();
                } catch (SQLException sqlEx) { }

                preparedStmt = null;
            }
        }

    }
}