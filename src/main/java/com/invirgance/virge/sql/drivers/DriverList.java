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
import static com.invirgance.virge.sql.DriverTools.COMMAND_DESCRIPTION_SPACING;
import static com.invirgance.virge.sql.DriverTools.COMMAND_SPACING;
import static com.invirgance.virge.sql.DriverTools.printToolOptions;
import com.invirgance.virge.tool.Tool;

/**
 *
 * @author tadghh
 */
public class DriverList implements Tool
{
    private String driver;
    
    @Override
    public String getName()
    {
        return "list";
    }

    @Override
    public String[] getHelp()
    {
        return new String[]{
           COMMAND_SPACING + "--driver <driver>",
           COMMAND_SPACING + "-d <driver>",
           COMMAND_SPACING + COMMAND_DESCRIPTION_SPACING  + "The name of the driver",
        };
    }
    
    @Override 
    public String getShortDescription()
    {
        return "List the available drivers for connecting to databases.";
    }
    
    @Override
    public boolean parse(String[] args, int start) throws Exception
    {
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
                    printToolOptions(this);    
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
        if(driver != null) printDriver(driver);
        else printAll();
    }
    
    public void printDriver(String driver)
    {
        JDBCDrivers drivers = new JDBCDrivers();
        JSONObject selected = drivers.getDescriptor(driver);
        
        if(selected == null) throw new ConvirganceException("Unknown driver: " + driver);
        
        System.out.println(selected.toString(4));
    }
    
    public void printAll()
    {
        JDBCDrivers drivers = new JDBCDrivers();
        
        // Print command widths (Name: 14, Example: 8)
        int[] widths = new int[]{ 14, 8 };
        
        String example;
        
        for(JSONObject descriptor : drivers)
        {
            example = !descriptor.getJSONArray("examples").isEmpty() ? descriptor.getJSONArray("examples").getString(0) : "";
            
            if(widths[0] < descriptor.getString("name").length()) widths[0] = descriptor.getString("name").length();
            if(widths[1] < example.length()) widths[1] = example.length();
        }
        
        System.out.print(formatWidth("Database Name", widths[0]));
        System.out.print("  ");
        System.out.println(formatWidth("Connection String Example", widths[1]));
        
        System.out.print(drawWidth('=', widths[0]));
        System.out.print("  ");
        System.out.println(drawWidth('=', widths[1]));
            
        for(JSONObject descriptor : drivers)
        {
            example = !descriptor.getJSONArray("examples").isEmpty() ? descriptor.getJSONArray("examples").getString(0) : "";
            
            System.out.print(formatWidth(descriptor.getString("name"), widths[0]));
            System.out.print("  ");
            System.out.println(formatWidth(example, widths[1]));
        }
    }
    
    private String formatWidth(String value, int width)
    {
        while(value.length() < width) value += " ";
        
        return value;
    }
    
    private String drawWidth(char c, int width)
    {
        StringBuffer buffer = new StringBuffer();
        
        while(buffer.length() < width) buffer.append(c);
        
        return buffer.toString();
    }
}
