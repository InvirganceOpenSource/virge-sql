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

package com.invirgance.virge.sql;

import static com.invirgance.virge.sql.VirgeSQL.SELECTED;
import static com.invirgance.virge.sql.VirgeSQL.print;
import com.invirgance.virge.sql.drivers.DriverList;
import com.invirgance.virge.sql.drivers.DriverRegister;
import com.invirgance.virge.sql.drivers.DriverUnregister;
import com.invirgance.virge.tool.Tool;
import java.util.ArrayList;
/**
 *
 * @author tadghh
 */
public class DriverTools implements Tool
{
    public static final String COMMAND_SPACING = "    ";
    public static final String COMMAND_DESCRIPTION_SPACING = "   ";
    private static final Tool[] TOOLS = new Tool[]{
        new DriverList(),
        new DriverRegister(),
        new DriverUnregister()
    };
     
    private Tool tool;
    
    public Tool[] getTools()
    {
        return TOOLS;
    }
    
    @Override
    public String getName()
    {
        return "drivers";
    }
    

    
    public static void printToolOptions(Tool selected)
    {
        
        System.out.println();
        System.out.println("Usage: virge.jar sql " + SELECTED.getName() + " " + selected.getName());
        System.out.println();
        System.out.println(selected.getShortDescription());
        System.out.println();
        System.out.println("Options:");
        System.out.println();
        
        print(selected.getHelp(), System.out);
        
        System.exit(1);
    }
    
    @Override
    public String[] getHelp()
    {
        ArrayList<String> help = new ArrayList<>();
        
        for(Tool tool : TOOLS)
        {
            help.add(COMMAND_SPACING + tool.getName() + " - " + tool.getShortDescription());
        }
        
        return help.toArray(new String[0]);
    }
    
    @Override
    public String getShortDescription()
    {
        return "List and manage available database drivers.";
    }
    
    @Override
    public boolean parse(String[] args, int start) throws Exception
    { 
        if(start == args.length) return false;
        if("-h".equals(args[start]) || "--help".equals(args[start])) return false;

        for(Tool tool : TOOLS)
        { 
            if(tool.getName().equals(args[start]))
            {
                this.tool = tool;
                
                if(!this.tool.parse(args, start + 1))
                {                    
                    if(args.length != start+1) System.err.println("\nUnknown option: " + args[start + 1]);
                    
                    printToolOptions(this.tool);
                }
                else
                {
                    return true;
                }
            }  
        }
        
        System.err.println("\nUnknown command: " + args[start]);
             
        return false;
    }

    @Override
    public void execute() throws Exception
    {
        tool.execute();
    }
    
}
