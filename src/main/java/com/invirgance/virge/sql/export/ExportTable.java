/*
 * Copyright 2024 INVIRGANCE LLC

Permission is hereby granted, free of charge, to any person obtaining a copy 
of this software and associated documentation files (the “Software”), to deal 
in the Software without restriction, including without limitation the rights to 
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies 
of the Software, and to permit persons to whom the Software is furnished to do 
so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
SOFTWARE.
 */
package com.invirgance.virge.sql.export;

import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.dbms.DBMS;
import com.invirgance.convirgance.dbms.Query;
import com.invirgance.convirgance.input.DelimitedInput;
import com.invirgance.convirgance.jdbc.AutomaticDrivers;
import com.invirgance.convirgance.jdbc.StoredConnection;
import com.invirgance.convirgance.jdbc.StoredConnections;
import com.invirgance.convirgance.output.CSVOutput;
import com.invirgance.convirgance.output.DelimitedOutput;
import com.invirgance.convirgance.output.JBINOutput;
import com.invirgance.convirgance.output.JSONOutput;
import com.invirgance.convirgance.output.Output;
import com.invirgance.convirgance.output.OutputCursor;
import com.invirgance.convirgance.target.FileTarget;
import com.invirgance.convirgance.target.OutputStreamTarget;
import com.invirgance.convirgance.target.Target;
import static com.invirgance.virge.Virge.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.Virge.HELP_SPACING;
import static com.invirgance.virge.Virge.exit;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;

/**
 * Exports table data to a target file.
 * @author tadghh
 */
public class ExportTable implements Tool
{
    private Target target;
    private Output output;

    private char outputDelimiter;
    private String tableName;
    
    private String jdbcURL;
    private String username;
    private String password;
    
    private StoredConnection connection;
    private String connectionName;

    private boolean error(String message)
    {
        System.err.println(message);
        
        return false;
    }
    
    @Override
    public String getName()
    {
        return "table";
    }
    
    @Override
    public String getShortDescription()
    {
        return "Export a tables data to a target file.";
    }
    
    @Override
    public String[] getHelp()
    {
        return new String[] {
            HELP_SPACING + "--output <PATH> or piped -",
            HELP_SPACING + "-o <PATH> or piped -",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "To specify the path of the output file",
            "",
            HELP_SPACING + "--output-type [FORMAT]",
            HELP_SPACING + "-i [FORMAT]",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Specify the format of the output file.",
            "",
            HELP_SPACING + "--output-delimiter [DELIMITER]",
            HELP_SPACING + "-S [DELIMITER]",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Set the column delimiter to use for the exported file.",
            "",
            HELP_SPACING + "--name <NAME>",
            HELP_SPACING + "-n <NAME>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Specifies the name of the table to export from.",
            "",           
            HELP_SPACING + "--help",
            HELP_SPACING + "-h",
            HELP_SPACING + HELP_DESCRIPTION_SPACING  + "Display this menu.",                 
            "",
            "Connection Options: ",
            "",
            HELP_SPACING + "Stored Connection:",
            "",
            HELP_SPACING + "--connection-name <STORED_CONNECTION>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "The name of the stored connection to use.",
            "",
            HELP_SPACING + "Manual:",
            "",
            HELP_SPACING + "--username <USERNAME>",
            HELP_SPACING + "-u <USERNAME>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "The username to use when logging into the database",
            "",
            HELP_SPACING + "--password <PASSWORD>",
            HELP_SPACING + "-p <PASSWORD>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "The password to use when logging into the database",
            "",
            HELP_SPACING + "--jdbc-url <URL>",
            HELP_SPACING + "-j <URL>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Alternate method of specifying the JDBC connection url ",
        };
    }

    @Override
    public boolean parse(String[] args, int start) throws Exception
    {
        if(start == args.length) return false;

        for(int i=start; i<args.length; i++)
        {
            // Handle single-letter params with no spaces in them
            if(args[i].length() > 2 && args[i].charAt(0) == '-' && Character.isLetterOrDigit(args[i].charAt(1)))
            {
                parse(new String[]{ args[i].substring(0, 2), args[i].substring(2) }, 0);
                
                continue;
            }
            
            switch(args[i])
            {                
                case "--output-delimiter":
                case "-S":
                    outputDelimiter = args[++i].charAt(0);
                    
                    if(output instanceof DelimitedInput) ((DelimitedInput)output).setDelimiter(outputDelimiter);
                    
                    break;
                    
                case "--output-type":
                case "-i":
                    output = getOutputType(args[++i]);
                    
                    if(output instanceof DelimitedInput) ((DelimitedInput)output).setDelimiter(outputDelimiter);
                    
                    break;
                    
                case "--output":
                case "-s":
                    target = getTarget(args[++i]);
                    
                    if(output == null) output = detectTarget(args[i]);
                        
                    break;
                                        
                case "--name":
                case "-n":
                    tableName = args[++i];
                    break;
                    
                case "--connection-name":
                    connectionName = args[++i];
                    break;
                    
                case "--jdbc-url":
                case "-j":
                    jdbcURL = args[++i];
                    break;                  
                    
                case "--username":
                case "-u":
                    username = args[++i];
                    break;
                    
                case "--password":
                case "-p":
                    password = args[++i];
                    break;
                    
                case "--help":
                case "-h":
                    printToolHelp(this);                    

                default:
                    System.err.println("Unknown parameter: " + args[i]);
                    printToolHelp(this);
                    
            }
        }
        
        if(tableName == null) return error("No table name specified, and cannot be inferred from source! Use -n to specify a name.");       
        if(target == null) return error("No source specified!");
        if(output == null) return error("No input type specified and unable to autodetect");
        
        if(connectionName != null)
        {
            connection = StoredConnections.getConnection(connectionName);
            
            if(connection == null)
            {
                exit(255, "Saved connection " + connectionName + " does not exist!");
            }
        }
        else
        {
            if(jdbcURL == null) return error("JDBC URL not specified!");
            if(username == null) return error("Username not specified!");
            
            connection = AutomaticDrivers.getDriverByURL(jdbcURL)
                    .createConnection(null)
                    .driver()
                    .url(jdbcURL)
                    .password(password)
                    .username(username)
                    .build();
        }
        
        return true;
    }  
    
    private Target getTarget(String path) throws MalformedURLException, IOException
    {
        File file;
        
        if(path.equals("-")) return new OutputStreamTarget(System.out);

        file = new File(path);
        
        if(!file.isFile())
        {
            System.err.println("File not found: " + path);
            
            throw new ConvirganceException("File not found: " + path);
        }
        
        if(tableName == null) tableName = file.getName();
        
        return new FileTarget(file);
    }
    
    private Output detectTarget(String path) throws MalformedURLException
    {
        path = path.toLowerCase();
        
        if(path.endsWith(".json")) return new JSONOutput();
        if(path.endsWith(".csv")) return new CSVOutput(); 
        if(path.endsWith(".jbin")) return new JBINOutput(true);
        
        return null;
    }
   
    @Override
    public void execute() throws Exception
    {           
        DBMS dbms; 

        dbms = connection.getDBMS();
        
        var sourceIterable = dbms.query(new Query("SELECT * FROM " + tableName)).iterator();
        
        try(OutputCursor cursor = output.write(target))
        {
            while(sourceIterable.hasNext()) cursor.write(sourceIterable.next());
        } 
        
        System.out.println("Export completed");
    }    
    
    private Output getOutputType(String type)
    {
        switch(type)
        {
            case "csv": 
                return new CSVOutput();
            
            case "tsv":
                return new DelimitedOutput('\t');
            
            case "pipe":
                return new DelimitedOutput('|');
            
            case "delimited":
                
                if(outputDelimiter != 0) return new DelimitedOutput(outputDelimiter);
                
                return new DelimitedOutput();
            
            case "jbin":
                return new JBINOutput();
                
            case "json":
                return new JSONOutput();
                
            default:
                exit(255, "Unknown input type: " + type);
                return null; // Keep the compiler happy
        }
    }    
    
}
