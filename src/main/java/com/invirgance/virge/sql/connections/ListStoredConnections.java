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

import com.invirgance.convirgance.jdbc.StoredConnection;
import com.invirgance.convirgance.jdbc.StoredConnections;
import static com.invirgance.virge.Virge.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.Virge.HELP_SPACING;
import com.invirgance.virge.sql.ConsoleOutputFormatter;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Virge CLI tool, lists the current stored connections.
 * 
 * @author tadghh
 */
public class ListStoredConnections implements Tool
{
    private String name;
    
    @Override
    public String getName()
    {
        return "list";
    }
    
    @Override
    public String getShortDescription()
    {
        return "Lists all the stored connections.";
    }
    
    @Override
    public String[] getHelp()
    {
        return new String[]{
           HELP_SPACING + "default*",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Lists all current stored connections.",         
           "",
           HELP_SPACING + "--name <NAME>",
           HELP_SPACING + "-n <NAME>",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "View detailed information about the connection.",         
           "",
           HELP_SPACING + "--help",
           HELP_SPACING + "-h",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Display this menu.",         
        };    
    }

    @Override
    public boolean parse(String[] args, int start) throws Exception
    {
        for(int i=start; i<args.length; i++)
        {
            switch(args[i])
            {
                case "--name":
                case "-n":
                    this.name = args[++i];
                    break;
                    
                case "--help":
                case "-h":
                    printToolHelp(this);    
                    break;
                    
                default:
                    return false;    
            }
        }
        
        return true;
    }

    @Override
    public void execute() throws Exception
    {
        if(this.name != null) getInfo();
        else listAll();
    }
    
    private void getInfo()
    {
        StoredConnection connection = StoredConnections.getConnection(this.name);
        
        System.out.println("Config: " + connection.getName());
        System.out.println(connection.toString());
    }
    
    /**
     * Prints out the name, driver and data source for the save stored connections.
     */
    public void listAll()
    {
        Iterator<StoredConnection> connections = StoredConnections.list().iterator();
        StoredConnection connection;
        
        List<String> names = new ArrayList<>();
        List<String> drivers = new ArrayList<>();
        List<String> datasources = new ArrayList<>();

        while(connections.hasNext()) 
        {
            connection = connections.next();
            
            names.add(connection.getName());
            drivers.add(connection.getDriver().getName());
            datasources.add(connection.getDataSource().getClass().getCanonicalName());
        }
        
        new ConsoleOutputFormatter()
                .addColumn("Connection Name", names)
                .addColumn("Driver", drivers)
                .addColumn("DataSource", datasources)
                .print();
    }
}
