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

import com.invirgance.convirgance.jdbc.AutomaticDriver;
import com.invirgance.convirgance.jdbc.AutomaticDrivers;
import com.invirgance.convirgance.jdbc.StoredConnection;
import com.invirgance.convirgance.jdbc.StoredConnections;
import com.invirgance.virge.Virge;
import static com.invirgance.virge.Virge.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.Virge.HELP_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;
import java.util.HashMap;

/**
 *
 * @author tadghh
 */
public class RegisterStoredConnection implements Tool
{
    private String username;
    private String name;
    private String password = "";
    private String url;
    
    private HashMap<String, String> extras = new HashMap<>();
    
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
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Provide an optional name for the connection, by default the Driver Name and the Current User will be used.",         
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
                    
                case "--help":
                case "-h":
                    printToolHelp(this);    
                    break;
                    
                default:
//                    System.out.println(args[i]);
//                    System.out.println(args[i++]);
//                    if(!args[i].contains("--")) return false;
//                    extras.put(args[i], args[++i]);
                return false;    
            }
        }
        
        if(this.username == null) Virge.exit(255, "Failed: Stored Connection can not be created without a username");
        if(this.url == null) Virge.exit(255, "Failed: The Stored Connection was not provided a url...");
        
        return true;
    }

    @Override
    public void execute() throws Exception
    {
        addConnection();
    }
    
    private void addConnection()
    {
        String name;
        StoredConnection.DataSourceConfig config;

        StoredConnection connection;
        AutomaticDriver driver = AutomaticDrivers.getDriverByURL(this.url);
        
        name = this.name == null ? driver.getName() + this.username : this.name;
        
        if(StoredConnections.getConnection(name) != null)
        {
            System.err.println("Failed: A connection with the name "+ name +" already exists.");
            return;
        }
        
        connection = driver.createConnection(name)
                .driver()
                .url(this.url)
                .username(this.username)
                .password(this.password)
                .build();
        
        if(connection.getConnection() == null)
        {
            // Note: should we test the connection? 
            // Meh offer connection test command? seems superfluous
            System.err.println("Warning: Connection test failed!");
            System.out.println();
        }
        
        connection.save();
        
        if(!this.extras.isEmpty())
        {
//            config = connection.getDataSourceConfig();
//
//            for(Map.Entry<String, String> entry : this.extras.entrySet())
//            {
//                String value = entry.getValue();
//                String oldKey = entry.getKey()+"";
//                String property = normalizeExtraOption(oldKey);
//                
//                System.out.println("");
//                System.out.println("PROPERTY");
//                System.out.println(property);
//                System.out.println(value);
//                System.out.println("VALUE");
//                System.out.println("");
//                config.setProperty(property, value);
//                
//            }
        }
        System.out.println("Saved new Stored Connection");
    }
    
    private String normalizeExtraOption(String option)
    {
        return option.substring(2);
    }
}
