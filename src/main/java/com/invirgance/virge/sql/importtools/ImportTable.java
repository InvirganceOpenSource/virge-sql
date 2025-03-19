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
package com.invirgance.virge.sql.importtools;

import com.invirgance.convirgance.CloseableIterator;
import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.dbms.AtomicOperation;
import com.invirgance.convirgance.dbms.BatchOperation;
import com.invirgance.convirgance.dbms.DBMS;
import com.invirgance.convirgance.dbms.Query;
import com.invirgance.convirgance.dbms.QueryOperation;
import com.invirgance.convirgance.dbms.TransactionOperation;
import com.invirgance.convirgance.input.CSVInput;
import com.invirgance.convirgance.input.DelimitedInput;
import com.invirgance.convirgance.input.Input;
import com.invirgance.convirgance.input.InputCursor;
import com.invirgance.convirgance.input.JBINInput;
import com.invirgance.convirgance.input.JSONInput;
import com.invirgance.convirgance.jdbc.AutomaticDrivers;
import com.invirgance.convirgance.jdbc.StoredConnection;
import com.invirgance.convirgance.jdbc.StoredConnections;
import com.invirgance.convirgance.jdbc.schema.Table;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.source.FileSource;
import com.invirgance.convirgance.source.InputStreamSource;
import com.invirgance.convirgance.source.Source;
import com.invirgance.convirgance.source.URLSource;
import com.invirgance.convirgance.transform.CoerceStringsTransformer;
import com.invirgance.virge.Virge;
import static com.invirgance.virge.Virge.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.Virge.HELP_SPACING;
import static com.invirgance.virge.Virge.exit;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.sql.generate.GenerateTable;
import com.invirgance.virge.tool.Tool;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author tadghh
 */
public class ImportTable implements Tool
{
    private Source source;
    private Input<JSONObject> input;

    private char inputDelimiter;
    private boolean truncate;
    private boolean detectTypes;
    private boolean createTable = false;
    private String tableName;
    
    private String jdbcURL;
    private String username;
    private String password;
    
    private StoredConnection connection;
    private String connectionName;
    
    private boolean isURL(String path)
    {
        try
        {
            new URL(path);
            return true;
        }
        catch (MalformedURLException e)
        {
            return false;
        }
    }
    
    private void autoSetTableName()
    {
        if(tableName.contains(".")) tableName = tableName.substring(tableName.lastIndexOf("/") + 1, tableName.indexOf('.'));
    }
    
    private Source getSource(String path) throws MalformedURLException, IOException
    {
        File file;
        URL url;
        
        if(path.equals("-")) return new InputStreamSource(System.in);

        if(isURL(path))
        {
            url = URI.create(path).toURL();
            
            if(tableName == null)
            {
                tableName = url.getFile();
            
                autoSetTableName();
            }
            
            return new URLSource(url);
        }
        
        file = new File(path);
        
        if(!file.isFile())
        {
            System.err.println("File not found: " + path);
            
            throw new ConvirganceException("File not found: " + path);
        }
        
        if(tableName == null)
        {
            tableName = file.getName();
            
            autoSetTableName();
        }
        
        return new FileSource(file);
    }
    
    // TODO: Improve auto-detection
    private Input<JSONObject> detectInput(String path) throws MalformedURLException
    {
        if(isURL(path))
        {
            path = URI.create(path).toURL().getFile();
        }
        
        path = path.toLowerCase();
        
        if(path.endsWith(".json")) return new JSONInput();
        if(path.endsWith(".csv")) return new DelimitedInput(','); // TODO: need to support proper CSV format
        if(path.endsWith(".jbin")) return new JBINInput();
        
        return null;
    }

    private boolean error(String message)
    {
        System.err.println(message);
        
        return false;
    }
    
    @Override
    public String getName()
    {
        return "load";
    }
    
    @Override
    public String getShortDescription()
    {
        return "Load a table from an input source.";
    }
    
    @Override
    public String[] getHelp()
    {
        return new String[] {
            HELP_SPACING + "--source <PATH> or piped data -",
            HELP_SPACING + "-s <PATH> or piped data -",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Alternate method of specifying the source file",
            "",
            HELP_SPACING + "--source-type [FORMAT]",
            HELP_SPACING + "-i [FORMAT]",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Specify the format of the input file. Currently supported options are json, csv, tsv, pipe, delimited, and jbin",
            "",
            HELP_SPACING + "--source-delimiter [DELIMITER]",
            HELP_SPACING + "-S [DELIMITER]",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Set the column delimiter if the source is a delimited file (e.g. , or |)",
            "",
            HELP_SPACING + "--detect-input-types",
            HELP_SPACING + "-a",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Detect the actual datatypes from the source file ex \"5\" would turn into an intergar",
            "",
            HELP_SPACING + "--name [NAME]",
            HELP_SPACING + "-n [NAME]",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Specifies the name of the table to import into, will default to the filename of the input file.",
            "",
            HELP_SPACING + "--create",
            HELP_SPACING + "-c",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Create the table if its missing (this is non-desctructive).",
            "",
            HELP_SPACING + "--truncate",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Truncate the table prior to loading. All existing DATA will be LOST!",
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
                case "--help":
                case "-h":
                    printToolHelp(this);
                
                case "--source-delimiter":
                case "-S":
                    inputDelimiter = args[++i].charAt(0);
                    
                    if(input instanceof DelimitedInput) ((DelimitedInput)input).setDelimiter(inputDelimiter);
                    
                    break;
                    
                case "--source-type":
                case "-i":
                    input = getInputType(args[++i]);
                    
                    if(input instanceof DelimitedInput) ((DelimitedInput)input).setDelimiter(inputDelimiter);
                    
                    break;
                    
                case "--name":
                case "-n":
                    tableName = args[++i];
                    break;
                    
                case "--truncate":
                    truncate = true;
                    break;
                    
                case "--connection-name":
                    connectionName = args[++i];
                    break;
                    
                case "--jdbc-url":
                case "-j":
                    jdbcURL = args[++i];
                    break;
                    
                case "--create":
                case "-c":
                    createTable = true;
                    break;
                    
                case "--username":
                case "-u":
                    username = args[++i];
                    break;
                    
                case "--password":
                case "-p":
                    password = args[++i];
                    break;
                    
                case "--detect-input-types":
                case "-a":
                    detectTypes = true;
                    break;     
                    
                case "--source":
                case "-s":
                    source = getSource(args[++i]);
                    
                    if(input == null) input = detectInput(args[i]);
                        
                    break;
                    
                default:
                    
                    if(checkUnnamedOptions(args[i])) 
                    {
                        break;
                    }
                    else
                    {
                        System.err.println("Unknown parameter: " + args[i]);
                        printToolHelp(this);
                    }
            }
        }
        
        if(tableName == null) return error("No table name specified, and cannot be inferred from source! Use -n to specify a name.");       
        if(source == null) return error("No source specified!");
        if(input == null) return error("No input type specified and unable to autodetect");
        
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
    
    // Below is an example of a command with un-named options, specifically the file path and database url. Users.json could also be pipe input like '-'
    // virge.jar sql import load --connection-name testName ./users.json "jdbc:postgresql://localhost:5432/testcustomers" -n customers -a
    private boolean checkUnnamedOptions(String option) throws MalformedURLException, IOException
    {
        if(source == null && (option.equals("-") || option.contains(".")))
        {
            source = getSource(option);

            if(input == null) input = detectInput(option);

            return true;
        }
        else if(jdbcURL == null && AutomaticDrivers.getDriverByURL(option) != null)
        {
            jdbcURL = option;
            
            return true;
        }
        
        return false;
    }
   
    
    private Query getInsertQuery() throws Exception
    {
        InputCursor<JSONObject> cursor = input.read(source);
        JSONObject record;
        
        StringBuffer sql = new StringBuffer("insert into ");
        int index = 0;
        
        sql.append(tableName);
        sql.append("(\n");

        try(CloseableIterator<JSONObject> iterator = cursor.iterator())
        {
            if(!iterator.hasNext()) return null;
            
            record = iterator.next();
        }
        
        for(String key : record.keySet())
        {
            if(index > 0) sql.append(",\n");
            
            sql.append("    ");
            sql.append(connection.getDriver().quoteIdentifier(key));
            index++;
        }
        
        sql.append("\n) VALUES (");
        
        index = 0;
        
        if(detectTypes) record = new CoerceStringsTransformer().transform(record);

        for(Object key : record.keySet())
        {
            if(index > 0) sql.append(",\n");
            
            sql.append("    ");
            sql.append(":");
            sql.append(key);
            
            index++;
        }
        
        sql.append(")");
        
        return new Query(sql.toString());
    }

    @Override
    public void execute() throws Exception
    {
        List<AtomicOperation> operations = new ArrayList<>();
        Iterable<JSONObject> sourceIterable;
        
        String createQuery;
        
        DBMS dbms; 
        TransactionOperation transaction;
        BatchOperation batch; 
        
        Query query = getInsertQuery();
        
        if(query == null) Virge.exit(5, "Source provided no records to load!");

        dbms = connection.getDBMS();
        
        sourceIterable = input.read(source);
        
        if(detectTypes) sourceIterable = new CoerceStringsTransformer().transform(sourceIterable);

        batch = new BatchOperation(query, sourceIterable);

        if(this.createTable && !checkIfTableExists())
        {
            createQuery = new GenerateTable().generateTableSQL(connection.getDriver(), source, input, tableName, detectTypes);
            operations.add(new QueryOperation(new Query(createQuery)));
        }
        
        if(this.truncate)
        {
            operations.add(new QueryOperation(new Query("truncate table " + tableName)));
        } 
        
        operations.add(batch);
        transaction = new TransactionOperation(operations.toArray(new AtomicOperation[operations.size()]));
        
        dbms.update(transaction);
        
        System.out.println("Import completed");
    }    
    
    private boolean checkIfTableExists() throws SQLException 
    {
        Table[] tables = connection.getSchemaLayout().getAllTables();
        
        for(Table table : tables)
        {
            if(table.getName().equals(tableName)) return true;
        }
        
        return false;
    }
    
    private Input<JSONObject> getInputType(String type)
    {
        switch(type)
        {
            case "csv": 
                return new CSVInput();
            
            case "tsv":
                return new DelimitedInput('\t');
            
            case "pipe":
                return new DelimitedInput('|');
            
            case "delimited":
                
                if(inputDelimiter != 0) return new DelimitedInput(inputDelimiter);
                
                return new DelimitedInput();
            
            case "jbin":
                return new JBINInput();
                
            case "json":
                return new JSONInput();
                
            default:
                exit(255, "Unknown input type: " + type);
                return null; // Keep the compiler happy
        }
    }
}
