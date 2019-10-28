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

        PreparedStatement prepared_stmt_DRG = null;
        PreparedStatement prepared_stmt_Hospital_Referral = null;
        PreparedStatement prepared_stmt_Provider = null;
        PreparedStatement prepared_stmt_Charges = null;


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

            String query_DRG = " insert into DRG (ID, DRG)"
                    + " values (?, ?)";

            String query_Hospital_Referral = " insert into Hospital_Referral (Hospital_Referral_State, Hospital_Referral_City)"
                    + " values (?, ?)";

            String query_Provider = " insert into Provider (Provider_ID, Provider_Name, Provider_Street_Address, "
                    + "Provider_City, Provide_State, Provider_Zip, Hospital_Referral_ID)"
                    + " values (?, ?, ?, ?, ?, ?)";

            String query_Charges = " insert into Charges (Total_Discharges, Average_Covered_Charges, Average_Total_Payments, "
                    + "Average_Medicare_Payments)"
                    + " values (?, ?, ?, ?)";

            prepared_stmt_DRG = conn.prepareStatement(query_DRG);
            prepared_stmt_Hospital_Referral = conn.prepareStatement(query_Hospital_Referral);
            prepared_stmt_Provider = conn.prepareStatement(query_Provider);
            prepared_stmt_Charges = conn.prepareStatement(query_Charges);


            // just skipping the header in csv file
            String heading = csvReader1.readLine();
            while ((line = csvReader1.readLine()) != null) {
                data = csv_split(line);

                int drg_id = Integer.parseInt(data.get(0).substring(0,3));
                String drg = data.get(0).substring(5);

                int provider_id = Integer.parseInt(data.get(1));
                String prvider_name = data.get(2);
                String provider_street_address = data.get(3);
                String provider_city = data.get(4);
                String provider_state = data.get(5);
                int provider_zip = Integer.parseInt(data.get(6));

                String referral_state = data.get(7).substring(0,2);
                String referral_city = data.get(7).substring(4);

                int total_discharges = Integer.parseInt(data.get(8));
                float avg_covered_charges = Float.parseFloat(data.get(9));
                float avg_total_payments = Float.parseFloat(data.get(10));
                float avg_medicare_payments = Float.parseFloat(data.get(11));

                // drg table
                prepared_stmt_DRG.setInt (1, drg_id);
                prepared_stmt_DRG.setString    (2, drg);

                // Hospital Referral table
                prepared_stmt_Hospital_Referral.setString (1, referral_state);
                prepared_stmt_Hospital_Referral.setString (2, referral_city);

                // provider table
                prepared_stmt_Provider.setInt (1, provider_id);
                prepared_stmt_Provider.setString (2, prvider_name);
                prepared_stmt_Provider.setString    (3, provider_street_address);
                prepared_stmt_Provider.setString (4, provider_city);
                prepared_stmt_Provider.setString    (5, provider_state);
                prepared_stmt_Provider.setInt  (6, provider_zip);

                // charges table
                prepared_stmt_Charges.setInt  (1, total_discharges);
                prepared_stmt_Charges.setFloat  (2, avg_covered_charges);
                prepared_stmt_Charges.setFloat  (3, avg_total_payments);
                prepared_stmt_Charges.setFloat  (4, avg_medicare_payments);

                // execute the prepared statement
                prepared_stmt_DRG.execute();
                prepared_stmt_Hospital_Referral.execute();
                prepared_stmt_Provider.execute();
                prepared_stmt_Charges.execute();


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

            if (prepared_stmt_DRG != null) {
                try {
                    prepared_stmt_DRG.close();
                } catch (SQLException sqlEx) { }

                prepared_stmt_DRG = null;
            }
            if (prepared_stmt_Hospital_Referral != null) {
                try {
                    prepared_stmt_Hospital_Referral.close();
                } catch (SQLException sqlEx) { }

                prepared_stmt_Hospital_Referral = null;
            }
            if (prepared_stmt_Provider != null) {
                try {
                    prepared_stmt_Provider.close();
                } catch (SQLException sqlEx) { }

                prepared_stmt_Provider = null;
            }
            if (prepared_stmt_Charges != null) {
                try {
                    prepared_stmt_Charges.close();
                } catch (SQLException sqlEx) { }

                prepared_stmt_Charges = null;
            }
        }

    }
}