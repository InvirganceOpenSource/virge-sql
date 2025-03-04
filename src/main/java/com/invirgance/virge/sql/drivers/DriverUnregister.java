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

import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.virge.jdbc.JDBCDrivers;
import static com.invirgance.virge.sql.VirgeSQL.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.HELP_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;

/**
 *
 * @author tadghh
 */
public class DriverUnregister implements Tool
{
    private String driver;
    
    @Override
    public String getName()
    {
        return "unregister";
    }

    @Override
    public String[] getHelp()
    {
        return new String[]
        {
            HELP_SPACING + "--driver <driver>",
            HELP_SPACING + "-d <driver>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "The name of the driver "
        };
    }
    
    @Override 
    public String getShortDescription()
    {
        return "Unregister a driver so it can no longer be used to create connections.";
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
                case "--driver":
                case "-d":
                    this.driver = args[++i];
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
        if(driver == null) printDriver(driver);
        else unregisterDriver(driver);
    }
    
    public void printDriver(String driver)
    {
        JDBCDrivers drivers = new JDBCDrivers();
        JSONObject selected = drivers.getDescriptor(driver);
        
        if(selected == null) throw new ConvirganceException("Unknown driver: " + driver);
        
        System.out.println(selected.toString(4));
    }
    
    public void unregisterDriver(String driver)
    {
        JDBCDrivers drivers = new JDBCDrivers();
        JSONObject descriptor = drivers.getDescriptor(driver);
        
        if(descriptor == null)
        {
            System.err.print("Driver '" + driver + "' not found!");
            System.exit(1);
        }
        
        System.out.println("Removed Driver: " + driver);
        drivers.deleteDescriptor(descriptor);
    }
}
