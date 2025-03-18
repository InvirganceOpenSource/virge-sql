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
 * Removes a driver, preventing further use of it within Virge.
 * Consider using this if you need to override one of the default drivers.
 * 
 * @author tadghh
 */
public class UnregisterDriver implements Tool
{
    private String name;
    
    @Override
    public String getName()
    {
        return "remove";
    }

    @Override
    public String[] getHelp()
    {
        return new String[]
        {
            HELP_SPACING + "--name <NAME>",
            HELP_SPACING + "-n <NAME>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "The name of the driver to remove.",
            "",           
            HELP_SPACING + "--help",
            HELP_SPACING + "-h",
            HELP_SPACING + HELP_DESCRIPTION_SPACING  + "Display this menu.",     
        };
    }
    
    @Override 
    public String getShortDescription()
    {
        return "Remove a driver so it can no longer be used within Virge.";
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
        
        if(name == null) Virge.exit(255, "Error: You must provide the name of the driver to unregister...");
        
        return true;
    }

    @Override
    public void execute() throws Exception
    {
        unregisterDriver(name);
    }
    
    @Override
    public String getExample()
    {
        return "virge.jar sql drivers remove -n <NAME>";
    }
    
    private void unregisterDriver(String name)
    {
        AutomaticDriver driver = AutomaticDrivers.getDriverByName(name);
        
        if(driver == null)
        {
            System.err.println("Driver '" + name + "' not found!");
            System.out.println("Hint: Use 'virge.jar sql drivers list' to view the name of each driver.");
        
            return;
        }
        
        driver.delete();      
        
        System.out.println("Removed Driver: " + name);
    }
}
