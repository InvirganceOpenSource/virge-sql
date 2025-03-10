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
import com.invirgance.convirgance.jdbc.datasource.DriverDataSource;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.source.FileSource;
import com.invirgance.convirgance.source.InputStreamSource;
import com.invirgance.convirgance.source.Source;
import com.invirgance.convirgance.transform.CoerceStringsTransformer;
import com.invirgance.virge.Virge;
import static com.invirgance.virge.Virge.exit;
import static com.invirgance.virge.sql.VirgeSQL.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.HELP_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.sql.generate.GenerateTable;
import com.invirgance.virge.tool.Tool;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;

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

    public Source getSource()
    {
        return source;
    }

    public void setSource(Source source)
    {
        this.source = source;
    }

    public Input<JSONObject> getInput()
    {
        return input;
    }

    public void setInput(Input<JSONObject> input)
    {
        this.input = input;
    }
    
    private boolean isURL(String path)
    {
        char c;
        
        if(!path.contains(":/")) return false;
        if(path.charAt(0) == ':') return false;
        
        for(int i=0; i<path.length(); i++)
        {
            c = path.charAt(i);
            
            if(c == ':') return (path.charAt(i+1) == '/');
                
            if(!Character.isLetter(c)) return false;
        }
        
        return false;
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
            
                if(tableName.contains(".")) tableName = tableName.substring(0, tableName.indexOf('.'));
            }

            return new InputStreamSource(url.openStream());
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
            
            if(tableName.contains(".")) tableName = tableName.substring(0, tableName.indexOf('.'));      
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
            HELP_SPACING + "--source <file path> or piped data <\"-\">",
            HELP_SPACING + "-s <file path> or piped data <\"-\">",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Alternate method of specifying the source file",
            "",
            HELP_SPACING + "--source-type <format>",
            HELP_SPACING + "-i <format>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Specify the format of the input file. Currently supported options are json, csv, tsv, pipe, delimited, and jbin",
            "",
            HELP_SPACING + "--source-delimiter <delimiter>",
            HELP_SPACING + "-S <delimiter>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Set the column delimiter if the source is a delimited file (e.g. , or |)",
            "",
            HELP_SPACING + "--detect-input-types",
            HELP_SPACING + "-auto",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Detect the actual datatypes from the source file ex \"5\" would turn into an intergar",
            "",
            HELP_SPACING + "--name <table name>",
            HELP_SPACING + "-n <table name>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Specifies the table to load. By default the input filename is used as the table name.",
            "",
            HELP_SPACING + "--create",
            HELP_SPACING + "-c",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Create the table if its missing (this is non-desctructive).",
            "",
            HELP_SPACING + "--truncate",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Truncate the table prior to loading. All existing DATA will be LOST!",
            "",
            HELP_SPACING + "--username <username>",
            HELP_SPACING + "-u <username>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "The username to use when logging into the database",
            "",
            HELP_SPACING + "--password <password>",
            HELP_SPACING + "-p <password>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "The password to use when logging into the database",
            "",
            HELP_SPACING + "--jdbc-url <connection url>",
            HELP_SPACING + "-j <connection url>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Alternate method of specifying the JDBC connection url ",
            "",           
            HELP_SPACING + "--help",
            HELP_SPACING + "-h",
            HELP_SPACING + HELP_DESCRIPTION_SPACING  + "Display this menu.",     
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
                case "-?":
                    printToolHelp(this);
                    return true;
                
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
                    
                    if(source == null && (args[i].equals("-") || args[i].contains(".")))
                    {
                        source = getSource(args[i]);
                    
                        if(input == null) input = detectInput(args[i]);

                        break;
                    }
                    else if(jdbcURL == null)
                    {
                        jdbcURL = args[i];
                    }
                    else
                    {
                        exit(255, "Unknown parameter: " + args[i]);
                    }
            }
        }
        
        if(tableName == null) return error("No table name specified, and cannot be inferred from source! Use -n to specify a name.");       
        if(source == null) return error("No source specified!");
        if(input == null) return error("No input type specified and unable to autodetect");
        if(jdbcURL == null) return error("JDBC URL not specified!");
        
        return true;
    }
    
    private String normalizeObjectName(String name)
    {
        if(this.jdbcURL.contains("jdbc:mysql")) return name;
        else if(this.jdbcURL.contains("jdbc:maria")) return "`" + name + "`";
        
        return "\"" + name + "\"";
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
            sql.append(normalizeObjectName(key));
            
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
        
        DatabaseMetaData metadata;
        GenerateTable create;
        String createQuery;
        
        TransactionOperation transaction;
        BatchOperation batch; 
        
        Query query = getInsertQuery();
        DataSource dataSource = DriverDataSource.getDataSource(jdbcURL, username, password);
        DBMS dbms = new DBMS(dataSource);
          
        if(query == null) Virge.exit(5, "Source provided no records to load!");
        
        sourceIterable = input.read(source);
        
        if(detectTypes) sourceIterable = new CoerceStringsTransformer().transform(sourceIterable);

        batch = new BatchOperation(query, sourceIterable);
        
        if(this.createTable)
        {
            metadata = dbms.getSource().getConnection().getMetaData();
            
            if(!checkIfTableExists(metadata))
            {
                create = new GenerateTable();
                createQuery = create.generateTableSQL(source, input, tableName, detectTypes);
                operations.add(new QueryOperation(new Query(createQuery)));
            }
        }
        
        if(this.truncate)
        {
            operations.add(new QueryOperation(new Query("truncate table " + tableName)));
        } 
        
        operations.add(batch);
        transaction = new TransactionOperation(operations.toArray(new AtomicOperation[operations.size()]));
        
        dbms.update(transaction);
    }    
    
    private boolean checkIfTableExists(DatabaseMetaData metaData) throws SQLException 
    {
        ResultSet tables = null;

        try 
        {
            // Note: Convirgance JDBC, Schema issue
            tables = metaData.getTables(null, null, tableName, new String[] {"TABLE"});
            if(tables.next()) return true;

            return false;
        } 
        finally 
        {
            if(tables != null) tables.close();
        }
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
            case "|":
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
