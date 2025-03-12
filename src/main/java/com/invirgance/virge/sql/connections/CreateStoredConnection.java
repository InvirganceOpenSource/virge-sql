/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
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
import java.sql.SQLException;

/**
 *
 * @author tadghh
 */
public class CreateStoredConnection implements Tool
{
    private String username;
    private String name;
    
    // Security issue?   
    private String password = "";
    
    private String url;
    
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
           HELP_SPACING + "default",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Displays the current DataSources.",
           "",
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
        
        try
        {
            connection.getConnection();
        }
        catch (SQLException ex)
        {
            // Note: should we test the connection? 
            // Meh offer connection test command? seems superfluous
            System.err.println("Warning: Connection test failed!");
            System.out.println();
            System.err.println("Stored Connection Exception: " + ex.toString());
            System.out.println();
        }
        
        connection.save();
        System.out.println("Saved new Stored Connection");
    }
}
