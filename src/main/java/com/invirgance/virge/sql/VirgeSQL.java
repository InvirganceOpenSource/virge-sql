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

import static com.invirgance.virge.Virge.exit;
import static com.invirgance.virge.Virge.printHelp;
import static com.invirgance.virge.Virge.printShortHelp;
import com.invirgance.virge.tool.Tool;
import java.util.HashMap;
import java.util.Map;

/**
 * This module is meant to be used with Virge, it provides a way to execute commands against SQL Servers
 * 
 * @author tadghh
 */
public class VirgeSQL 
{
    public static final Map<String,Tool> lookup = new HashMap<>();
    
    public static final Tool[] tools = new Tool[] {
        new SQLDrivers()
    }; 
     
    static {
        for(Tool tool : tools) lookup.put(tool.getName(), tool);
    }
 
    public static void main(String[] args) throws Exception
    {
        Tool tool;
        
        if(args.length <= 1) printShortHelp();

        if(args[0].equals("--help") || args[0].equals("-h") || args[0].equals("-?"))
        {
            printHelp(null);
        }
        
        tool = lookup.get(args[0]);
        
        if(tool == null) exit(6, "Unknown tool: " + args[0]);
        
        if(!tool.parse(args, 1)) printHelp(tool);
        
        tool.execute();
    }

}
