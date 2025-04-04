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

import static com.invirgance.virge.Virge.HELP_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.sql.export.ExportTable;
import com.invirgance.virge.tool.Tool;
import java.util.ArrayList;

/**
 * Virge module, for SQL export tools.
 * @author tadghh
 */
public class ExportTools  implements Tool
{
    private Tool tool;
    
    private static final Tool[] TOOLS = new Tool[]{
        new ExportTable()
    };
    
    @Override
    public String getName()
    {
        return "export";
    }

    @Override
    public String[] getHelp()
    {
        ArrayList<String> help = new ArrayList<>();
        
        for(Tool tool : TOOLS)
        {
            help.add(HELP_SPACING + tool.getName() + " - " + tool.getShortDescription());
        }
        
        return help.toArray(new String[help.size()]);
    }
    
    @Override
    public String getShortDescription()
    {
        return "Export data from existing table(s).";
    }
    
    @Override
    public boolean parse(String[] args, int start) throws Exception
    { 
        if(start == args.length) return false;
        else if("-h".equals(args[start]) || "--help".equals(args[start])) return false;

        for(Tool tool : TOOLS)
        { 
            if(tool.getName().equals(args[start]))
            {
                this.tool = tool;
                
                if(!this.tool.parse(args, start + 1)) printToolHelp(this.tool);               
                else return true;
            }  
        }
        
        System.err.println("\nUnknown Tool: " + args[start]);
             
        return false;
    }

    @Override
    public void execute() throws Exception
    {
        tool.execute();
    }
    
}
