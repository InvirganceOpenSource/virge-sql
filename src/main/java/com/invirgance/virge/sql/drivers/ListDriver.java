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
import com.invirgance.virge.Virge;
import static com.invirgance.virge.Virge.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.Virge.HELP_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;

/**
 * Lists the registered database drivers.
 * 
 * @author tadghh
 */
public class ListDriver implements Tool
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
           HELP_SPACING + "default",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Provides a brief overview and connection templates of the current drivers.",
           "",
           HELP_SPACING + "--name <NAME>",
           HELP_SPACING + "-n <NAME>",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "View detailed information about a driver.",         
           "",
           HELP_SPACING + "--help",
           HELP_SPACING + "-h",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Display this menu.",         
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
                case "--name":
                case "-n":
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
    public String getExample()
    {
        return "virge.jar sql drivers list -n <driver name>";
    }
    
    @Override
    public void execute() throws Exception
    {
        if(driver != null) printDriver(driver);
        else printAll();
    }
    
    /**
     * Prints out detailed information about the driver.
     * This includes: 
     * - Driver
     * - DataSource
     * - URL prefix
     * - Keys
     * - Examples
     * 
     * @param driver The drivers name.
     */
    public void printDriver(String driver)
    {
        // Note: this is case-insensitve
        AutomaticDriver selected = AutomaticDrivers.getDriverByName(driver);

        if(selected == null) 
        {
            System.out.println();
            System.out.println("View the current driver names below:");
            printAll();
            System.out.println();
            Virge.exit(254, "Unknown driver name: " + driver);
        }
        
        System.out.println(selected.toString());
    }
    
    /**
     * Prints out high level information for the registered drivers.
     */
    public void printAll()
    {
        AutomaticDrivers drivers = new AutomaticDrivers();
        
        int[] widths = new int[]{ 14, 8 };
        
        String example;
        
        for(AutomaticDriver descriptor : drivers)
        {
            example = !(descriptor.getExamples().length == 0) ? descriptor.getExamples()[0] : "";
            
            if(widths[0] < descriptor.getName().length()) widths[0] = descriptor.getName().length();
            if(widths[1] < example.length()) widths[1] = example.length();
        }
        
        System.out.print(formatWidth("Database Name", widths[0]));
        System.out.print("  ");
        System.out.println(formatWidth("Connection String Example", widths[1]));
        
        System.out.print(drawWidth('=', widths[0]));
        System.out.print("  ");
        System.out.println(drawWidth('=', widths[1]));
            
        for(AutomaticDriver descriptor : drivers)
        {
            example = !(descriptor.getExamples().length == 0) ? descriptor.getExamples()[0] : "";
            
            System.out.print(formatWidth(descriptor.getName(), widths[0]));
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
