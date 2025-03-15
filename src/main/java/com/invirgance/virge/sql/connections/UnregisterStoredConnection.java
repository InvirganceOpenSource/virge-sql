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
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;

/**
 *
 * @author tadghh
 */
public class UnregisterStoredConnection implements Tool
{
    private String name;
    
    @Override
    public String getName()
    {
        return "remove";
    }
    
    @Override 
    public String getShortDescription()
    {
        return "Remove a Stored Connection.";
    }
    
    @Override
    public String[] getHelp()
    {
        return new String[]{
           HELP_SPACING + "--name <NAME>",
           HELP_SPACING + "-n <NAME>",
           HELP_SPACING + HELP_DESCRIPTION_SPACING + "The name of the connection to remove.",         
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
                case "--name":
                case "-n":
                    this.name = args[++i];
                    break;
                    
                case "--help":
                case "-h":
                    printToolHelp(this);    
                    break;
                    
                default:
                    System.err.println("\nUnknown option: " + args[i]);
                    
                    return false;    
            }
        }
        
        return true;
    }

    @Override
    public void execute() throws Exception
    {
        removeConnection();
    }
    
    private void removeConnection()
    {
        StoredConnection driver = StoredConnections.getConnection(this.name);
        
        driver.delete();
             
        System.out.println("Removed Stored Connection: " + this.name);
    }
}
