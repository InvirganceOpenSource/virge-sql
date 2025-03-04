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

import com.invirgance.virge.tool.Tool;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;

/**
 * This module is meant to be used with Virge, it provides a way to execute commands against SQL Servers
 * 
 * @author tadghh
 */
public class VirgeSQL 
{
    public static Tool SELECTED;
    
    public static final Map<String,Tool> lookup = new HashMap<>();
        
    public static final Tool[] tools = new Tool[] {
        new DriverTools()
    }; 
     
    static {
        for(Tool tool : tools) lookup.put(tool.getName(), tool);
    }
    
    public static void print(String[] lines, PrintStream out)
    {
        for(String line : lines)
        {
            out.println(line);
        }
        
        out.println();
        out.println();
    }
    
    public static void printHelp(Tool selected)
    {
        
        System.out.println();
        System.out.println("Usage: virge.jar sql " + SELECTED.getName() + " " + selected.getName());
        System.out.println();
        System.out.println("Commands:");
        System.out.println();
        
        print(selected.getHelp(), System.out);
        
        System.exit(1);
    }
    
    public static void printModuleHelp()
    {
        String command = SELECTED != null ? SELECTED.getName() + " " : "";
        
        System.out.println();
        System.out.println("Usage: virge.jar sql " + command);
        System.out.println();
        System.out.println("Commands:");
        System.out.println();
        
        if(SELECTED != null)
        {
            print(SELECTED.getHelp(), System.out);
        }
        else
        {
            for(Tool help : tools)
            {
                System.out.println(help.getShortDescription());
            }
            
            System.out.println();
        }
              
        System.exit(1);
    }
    
    public static void main(String[] args) throws Exception
    {
        // NOTE: -? might be a special pattern in some shells, zsh?
        if(args.length == 0 || args[0].equals("--help") || args[0].equals("-h") || args[0].equals("-?"))
        {   
            printModuleHelp();
         
            return;
        }
        
        SELECTED = lookup.get(args[0]);

        if(SELECTED == null) 
        {
            System.out.println("\n\u001B[33mUnknown Tool:\u001B[0m " + args[0]);
            
            printModuleHelp();
        }
        
        if(!SELECTED.parse(args, 1)) printModuleHelp();
        
        SELECTED.execute();
    }

}
