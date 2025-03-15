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

package com.invirgance.virge.sql.drivers;

import com.invirgance.convirgance.jdbc.AutomaticDriver;
import com.invirgance.convirgance.jdbc.AutomaticDrivers;
import com.invirgance.convirgance.jdbc.datasource.DataSourceManager;
import static com.invirgance.virge.Virge.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.Virge.HELP_SPACING;
import com.invirgance.virge.sql.ConsoleOutputFormatter;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Displays information about DataSource properties.
 * Use this when assigning the properties of a DataSource for a StoredConnection.
 * 
 * @author tadghh
 */
public class ListDataSource implements Tool
{
    private String sourceName;
    
    @Override
    public String getName()
    {
        return "datasource";
    }
    
    @Override 
    public String getShortDescription()
    {
        return "List the available DataSources and their properties.";
    }
    
    @Override
    public String[] getHelp()
    {
        return new String[]{
           HELP_SPACING + "default",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Displays the current DataSources.",
           "",
           HELP_SPACING + "--name [NAME]",
           HELP_SPACING + "-n [NAME]",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "View a DataSource's properties.",         
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Ex: -n OracleDataSource",         
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
                    this.sourceName = args[++i];
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
        if(this.sourceName != null) printDataSourceProperties();
        else displayDataSources();
    }
 
    private void displayDataSources()
    {
        AutomaticDriver driver;
        Iterator<AutomaticDriver> drivers = new AutomaticDrivers().iterator();

        List<String> names = new ArrayList<>();
        List<String> canonical = new ArrayList<>();

        while(drivers.hasNext()) 
        {
            driver = drivers.next();

            names.add(driver.getName());
            canonical.add(driver.getDataSource().getClass().getCanonicalName());
        }
        
        new ConsoleOutputFormatter()
                .addColumn("Driver Name", names)
                .addColumn("Data Source", canonical)
                .print();
    }
    
    private void printDataSourceProperties()
    {
        AutomaticDriver driver;
        
        String simple;

        Iterator<AutomaticDriver> drivers = new AutomaticDrivers().iterator();
        DataSourceManager manager;
        
        while(drivers.hasNext()) 
        {
            driver = drivers.next();
            simple = driver.getDataSource().getClass().getSimpleName();
            
            if(simple.equals(this.sourceName)) 
            {
                manager = new DataSourceManager(driver.getDataSource());
                
                System.out.println(driver.getName() + " (" + driver.getDataSource().getClass().getName() + ")");
                System.out.println(HELP_SPACING + "Properties:");
                
                for(String properties : manager.getProperties()) 
                {
                    System.out.println(HELP_SPACING + HELP_DESCRIPTION_SPACING + " " + properties);
                }
                
                return;
            }
        }  
        
        System.err.println("Unknown DataSource: " + this.sourceName);
        System.err.println("Hint: Run this command without any options to view all DataSources.");
    }

}
