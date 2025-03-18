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
package com.invirgance.virge.sql.connections;

import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.jdbc.AutomaticDriver;
import com.invirgance.convirgance.jdbc.AutomaticDrivers;
import com.invirgance.convirgance.jdbc.StoredConnection;
import com.invirgance.convirgance.jdbc.StoredConnections.DataSourceConfigBuilder;
import com.invirgance.convirgance.jdbc.datasource.DataSourceManager;
import com.invirgance.virge.Virge;
import static com.invirgance.virge.Virge.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.Virge.HELP_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Virge CLI tool for registering stored connections
 * 
 * @author tadghh
 */
public class RegisterStoredConnection implements Tool
{
    private boolean isDataSource = false;
    private boolean skipConnectionTest = false;
    private StoredConnection storedConnection;
    private String username;
    private String name;
    private String password = "";
    private String url;
    private String database;

    private final HashMap<String, String> extras = new HashMap<>();
    
    @Override
    public String getName()
    {
        return "add";
    }
    
    @Override 
    public String getShortDescription()
    {
        return "Create a new Stored Connection.";
    }
    
    @Override
    public String[] getHelp()
    {
        return new String[]{
           HELP_SPACING + "--username <USERNAME>",
           HELP_SPACING + "-u <USERNAME>",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Connection username.",
           "",
           HELP_SPACING + "--password [PASSWORD]",
           HELP_SPACING + "-p [PASSWORD]",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Password of the user, used for the connection.",         
           "",
           HELP_SPACING + "--connection-url <JDBC_URL>",
           HELP_SPACING + "-c <JDBC_URL>",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "The JDBC URL to connect with.",         
           "",
           HELP_SPACING + "--name [NAME]",
           HELP_SPACING + "-n [NAME]",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Provide an optional name for the stored connection, by default the Driver Name and the User will be used.",         
           "",
           HELP_SPACING + "--database <[TYPE]>",
           HELP_SPACING + "-d <[TYPE]>",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Set the type of database to create a stored connection (config) for.",         
           "",
           HELP_SPACING + "--type-datasource",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Toggle this flag to add the StoredConnection as a data source config.",         
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Enabling this will also allow you to add datasource specific properties with '--'",         
           "",
           HELP_SPACING + "--skip-test",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Skips testing the Stored Connection.",         
           "",
           HELP_SPACING + "--help",
           HELP_SPACING + "-h",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Display this menu.",         
        };
    }

    @Override
    public boolean parse(String[] args, int start) throws Exception
    {
        // No parameter
        if(start == args.length) return false;
        
        for(int i=start; i<args.length; i++)
        {
            switch(args[i])
            {
                case "--username":
                case "-u":
                    this.username = args[++i];
                    break;
                    
                case "--name":
                case "-n":
                    this.name = args[++i];
                    break;
                    
                case "--password":
                case "-p":
                    this.password = args[++i];
                    break;
                    
                case "--connection-url":
                case "-c":
                    this.url = args[++i];
                    break;
                    
                case "--database":
                case "-d":
                    this.database = args[++i];
                    break;
                    
                case "--type-datasource":
                    this.isDataSource = true;
                    break;
                    
                case "--skip-test":
                    this.skipConnectionTest = true;
                    break;
                    
                case "--help":
                case "-h":
                    printToolHelp(this);    
                    break;
                    
                default:
                    
                    if(!checkOptionFallThrough(args, i)) return false; 
                    
                    ++i;
            }
        }
        
        if(this.isDataSource && this.database == null)
        {
            Virge.exit(255, "Failed: Attempted to create a stored connection without a selected database...");
        }
        else if(!this.isDataSource)
        {
            if(this.username == null) Virge.exit(255, "Failed: Stored Connection can not be created without a username");
            if(this.url == null) Virge.exit(255, "Failed: The Stored Connection was not provided a url...");  
        }

        return true;
    }
    
    @Override
    public void execute() throws Exception
    {
        if(this.isDataSource) addDataSourceConnection();
        else addConnection();
        
        System.out.println("Saved new Stored Connection (type: " + connectionType() + "): " + this.storedConnection.getName());       
        System.out.println(this.storedConnection.toString());       
    }
    
    // For handling datasource specific parameters, these being with '--'
    // ex: '--enableFastMode' 
    // virge.jar sql connection add -n testName --type-datasource --database PostgreSQL --sendBufferSize 1
    private boolean checkOptionFallThrough(String[] args, int index)
    {
        String option;
        
        if(this.isDataSource && index != args.length)
        {
            option = args[index];
            String value;

            if(!option.startsWith("--")) 
            {
                System.err.println("Unknown option: " + option);

                return false;
            }

            if(index + 1 < args.length)
            {
                value = args[++index];
                extras.put(option, value);
            }
            else
            {
                System.err.println("Missing value for: " + option);
                return false;
            }   
        }
        else
        {   
            System.err.println("Unknown option: " + args[index]);
            System.out.println("Hint: Are you trying to configure a datasource? add --type-datasource");
            
            return false;
        }
        
        return true;
    }
    
    private void addDataSourceConnection()
    {  
        DataSourceConfigBuilder connection;
        
        AutomaticDriver driver = AutomaticDrivers.getDriverByName(this.database);

        connection = driver.createConnection(createNewConnectionName(driver)).datasource();
        
        // Verify valid property
        DataSourceManager manager;        
        List<String> properties;
        
        if(!this.extras.isEmpty())
        {
            manager = new DataSourceManager(driver.getDataSource());
            properties = Arrays.asList(manager.getProperties());
            
            for(Map.Entry<String, String> entry : this.extras.entrySet())
            {    
                String key = normalizeExtraOption(entry.getKey());
                
                if(!properties.contains(key))
                {
                    Virge.exit(255, "Unknown datasource option for " + this.database + ": " + key);
                    return;
                }
                
                connection.property(key, entry.getValue());           
            }   
        }  
        
        this.storedConnection = connection.build();
        this.storedConnection.save(); 
    }
    
    private void addConnection()
    {
        AutomaticDriver driver = AutomaticDrivers.getDriverByURL(this.url);
        
        this.storedConnection = driver.createConnection(createNewConnectionName(driver))
                .driver()
                .url(this.url)
                .username(this.username)
                .password(this.password)
                .build();
        
        if(!this.skipConnectionTest) testStoredConnection(this.storedConnection);

        this.storedConnection.save();
    }
 
    // Returns the type of stored connection that is being created
    private String connectionType()
    {
        return this.isDataSource ? "DataSource" : "Driver";
    }
    
    // Creates a default name when no name is provided
    private String createNewConnectionName(AutomaticDriver driver)
    {
        String suffix = "";
        
        if(this.username != null) suffix = this.username;
        
        return this.name == null ? driver.getName() + suffix : this.name;
    }
    
    private String normalizeExtraOption(String option)
    {
        if(option.startsWith("--")) return option.substring(2);
        
        return option;
    }
    
    /**
     * Validates that the stored connection can communicate with its relevant database.
     * 
     * @param storedConnection The connection to validate
     * @throws ConvirganceException If the connection fails
     */
    public void testStoredConnection(StoredConnection storedConnection) throws ConvirganceException
    {
        storedConnection.execute(connection -> {
            if(connection == null || !connection.isValid(10)) throw new ConvirganceException("Unable to connect to database server");
        });
    }
}
