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

package com.invirgance.virge.sql.datasource;

import com.invirgance.convirgance.jdbc.AutomaticDriver;
import com.invirgance.convirgance.jdbc.AutomaticDrivers;
import com.invirgance.convirgance.jdbc.datasource.DataSourceManager;
import static com.invirgance.virge.Virge.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.Virge.HELP_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;
import java.util.Iterator;

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
        DataSourceManager manager;
        Iterator<AutomaticDriver> drivers;
        AutomaticDriver driver;
        drivers = new AutomaticDrivers().iterator();

        System.out.println("Data Sources:");
        
        while(drivers.hasNext()) 
        {
            driver = drivers.next();

            manager = new DataSourceManager(driver.getDataSource());
            
            System.out.println(HELP_SPACING + manager.getDataSource().getClass().getName());
        }
    }
    
    private void printDataSourceProperties()
    {
        DataSourceManager manager;
        Iterator<AutomaticDriver> drivers = new AutomaticDrivers().iterator();
        AutomaticDriver driver;
        String simple;
        
        while(drivers.hasNext()) 
        {
            driver = drivers.next();
            simple = driver.getDataSource().getClass().getSimpleName();
            
            if(simple.equals(this.sourceName)) 
            {
                manager = new DataSourceManager(driver.getDataSource());
                
                System.out.println(driver.getName() + " (" + driver.getDataSource().getClass().getName() + ")");
                System.out.println();
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
