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
import com.invirgance.virge.sql.ConsoleOutputFormatter;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;
import java.util.ArrayList;
import java.util.List;

/**
 * Lists the drivers registered with Virge.
 * 
 * @author tadghh
 */
public class ListDriver implements Tool
{
    private String name;
    
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
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "Lists the driver names and connection url examples.",
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
                    if(i + 1 < args.length) this.name = args[++i];
                    break;
                    
                case "--help":
                case "-h":
                    printToolHelp(this);    
                    
                default:
                    System.err.println("Unknown option: " + args[i]);
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
        if(name != null) printDriver();
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
    private void printDriver()
    {
        // Note: this is case-insensitve
        AutomaticDriver selected = AutomaticDrivers.getDriverByName(name);

        if(selected == null) 
        {
            System.out.println();
            System.out.println("View the current driver names below:");
            printAll();
            System.out.println();
            Virge.exit(254, "Unknown driver name: " + name);
        }
        
        System.out.println(selected.toString());
    }
    
    /**
     * Prints out high level information for the registered drivers.
     */
    public void printAll()
    {
        String example;
        
        List<String> names = new ArrayList<>();
        List<String> examples = new ArrayList<>();
        
        AutomaticDrivers drivers = new AutomaticDrivers();
        
        for(AutomaticDriver descriptor : drivers)
        {
            example = !(descriptor.getExamples().length == 0) ? descriptor.getExamples()[0] : "";
            
            examples.add(example);
            names.add(descriptor.getName());
        }
        
        new ConsoleOutputFormatter()
                .addColumn("Driver name", names)
                .addColumn("Connection String Example", examples)
                .print();
    }
}
