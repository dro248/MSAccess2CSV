import com.healthmarketscience.jackcess.*;
import com.healthmarketscience.jackcess.util.ExportUtil;
import com.healthmarketscience.jackcess.util.SimpleExportFilter;
import org.hsqldb.util.CSVWriter;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class TableExtractor {
    /**
     * Prints all tables to the console
     * @param db
     */
    public static void list_tables(Database db){
        try {
            for (String table_name : db.getTableNames()) {
                System.out.println(table_name);
            }
        }
        catch(Exception e){
            System.out.println("ERROR::: Invalid Database passed.");
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Writes a table to a CSV (at the given output path) - output_path/table_name.csv
     * @param db
     * @param output_path
     * @param table_name
     */
    public static void write_table_to_csv(Database db, File output_path, String table_name, boolean verbose){
        try{
            String csvName = table_name + ".csv";
            File output_csv_file = new File(output_path.toString(), csvName);

            ExportUtil.exportFile(
                    db,
                    table_name,
                    output_csv_file,
                    true,
                    null,
                    '"',
                    SimpleExportFilter.INSTANCE);

            System.out.println(table_name + " exported to \"" + output_csv_file.toString() + "\".");
        }
        catch(Exception e){
            System.out.println("Failed to export \"" + table_name + "\" to csv.");
            if(verbose) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Writes all tables to their respective csv (e.g. Sales_Activity table => "Sales_Activity.csv")
     * @param db
     */
    public static void write_all_tables_to_csv(Database db, File output_path, boolean verbose){
        try {
            for(String table_name : db.getTableNames()){
                write_table_to_csv(db, output_path, table_name, verbose);
            }
        }
        catch(Exception e){
            System.out.println("Error during writing ALL TABLES to CSV");
            if (verbose){
                e.printStackTrace();
            }
        }
    }







    /**
     * Expects the filename (as a String)
     * @param database_filename
     * @return - returns a Database object if successful, otherwise throws an IOException
     */
    private static Database get_database(String database_filename){
        File db_file = new File(database_filename);

        try {
            // Validate db_file
            if (!db_file.isFile()){
                throw new IllegalArgumentException("ERROR::: Invalid Database file supplied.");
            }
            return DatabaseBuilder.open(db_file);
        }
        catch (IOException e){
            System.out.println("Caught an error attempting to read the database.");
            e.printStackTrace();
            System.exit(-1);
        }
        catch (IllegalArgumentException e){
            e.printStackTrace();
            System.exit(-1);
        }

        return null;
    }


    private static void print_usage(){
        System.out.println("USAGE:");
        System.out.println("======");
        System.out.println(" * HELP: java -jar AccessTableExtractor [no input | -h | --help] ");
        System.out.println(" * LIST TABLES: java -jar AccessTableExtractor [-l | --list_tables] --input [PATH_TO_DB.accdb|*.mdb]");
        System.out.println(" * WRITE SINGLE TABLE: java -jar AccessTableExtractor --input [PATH_TO_DB.accdb|*.mdb] --output [PATH_TO_OUTPUT_DIRECTORY] --export_table [TABLE_NAME]");
        System.out.println(" * WRITE ALL TABLES: java -jar AccessTableExtractor --input [PATH_TO_DB.accdb|*.mdb] --output [PATH_TO_OUTPUT_DIRECTORY] --export_all_tables");
        System.out.println();
        System.out.println();
        System.out.println("Additional Flags:");
        System.out.println("+ VERBOSE: [-v | --verbose] - shows export error stack trace (if they exist).");
    }




    public static void main(String[] args) {
        List<String> args_list = Arrays.asList(args);
        Database db = null;     // --input
        File output_dir = null; // --output
        String export_tablename = null; // --export_table
        boolean verbose = false;

        //////////////////////////////
        //      PARSE ARGS          //
        //////////////////////////////

        // HELP flag passed
        if (args_list.isEmpty() || args_list.contains("-h") || args_list.contains("--help")){
            print_usage();
        }

        // Validate INPUT flag
        if (args_list.contains("--input")){
            // Get the index of the INPUT flag
            int flag_index = args_list.indexOf("--input");

            // Check that an INPUT exists
            try {
                String database_filename = args_list.get(flag_index+1);
                db = get_database(database_filename);
            }
            catch(Exception e){
                // INDEX OUT OF BOUNDS ERROR
                System.out.println("Input database not supplied.");
                e.printStackTrace();
                System.exit(-1);
            }
        }

        // Validate OUTPUT flag
        if (args_list.contains("--output")){
            // Get the index of the OUTPUT flag
            int flag_index = args_list.indexOf("--output");

            // Check that an OUTPUT exists
            try {
                String output_dir_str = args_list.get(flag_index+1);
                output_dir = new File(output_dir_str);

                if (!output_dir.isDirectory()){
                    throw new FileNotFoundException("ERROR::: Output must be a valid DIRECTORY.");
                }
            }
            catch(Exception e){
                // INDEX OUT OF BOUNDS ERROR
                System.out.println("Invalid output directory or output directory not supplied.");
                e.printStackTrace();
                System.exit(-1);
            }
        }

        // VERBOSE flag was passed
        if (args_list.contains("-v") || args_list.contains("--verbose")){
            verbose = true;
        }

        // LIST TABLES flag was passed
        if (args_list.contains("-l") || args_list.contains("--list_tables")){
            list_tables(db);
            System.exit(0);
        }

        // EXPORT TABLE flag was passed
        if (args_list.contains("--export_table")){
            // Check that a valid table is passed
            int flag_index = args_list.indexOf("--export_table");
            try {
                export_tablename = args_list.get(flag_index+1);
                boolean found_flag = false;

                // Search for the provided tablename among the actual table names; Export it if it exists.
                for(String name : db.getTableNames()){
                    if (export_tablename.toLowerCase().equals(name.toLowerCase())){
                        found_flag = true;
                        write_table_to_csv(db, output_dir, name, verbose);
                        System.exit(0);
                    }
                }

                // If the given tablename doesn't exist, notify the user
                if (!found_flag){
                    System.out.println("ERROR::: Provided table name {" + export_tablename + "} was not found.");
                    System.exit(-1);
                }
            }
            catch (Exception e){
                System.out.println("ERROR::: Export table name not supplied.");
                e.printStackTrace();
                System.exit(-1);
            }
        }


        // EXPORT ALL TABLES flag was passed
        if (args_list.contains("--export_all_tables")){
            write_all_tables_to_csv(db, output_dir, verbose);
            System.exit(0);
        }
    }
}

