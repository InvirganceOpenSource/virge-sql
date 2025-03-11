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
    public static final String HELP_SPACING = "    ";
    public static final String HELP_DESCRIPTION_SPACING = "   ";
    
    private static final String HELP = "Tools for interacting with SQL Databases.";
    
    public static Tool SELECTED;
    
    public static final Map<String,Tool> lookup = new HashMap<>();
        
    public static final Tool[] tools = new Tool[] {
        new DriverTools(),
        new GenerationTools(),
        new ImportTools()
    }; 
     
    static {
        for(Tool tool : tools) lookup.put(tool.getName(), tool);
    }
    
    private static void print(String[] lines, PrintStream out)
    {
        for(String line : lines)
        {
            out.println(line);
        }
        
        out.println();
    }
    
    /**
     * Prints out the help for:
     * - Top level commands
     * - Sub level command options
     * - When no command is provided
     * 
     * If this module is called without any tool, than the help for this specific module is printed
     * 
     * @param selected the selected sub command or null for top level commands
     */
    public static void printToolHelp(Tool selected)
    {
        // TODO look at adding sub tools to Tool
        Boolean level = SELECTED != null && selected != null && !SELECTED.getName().equals(selected.getName()) || SELECTED != null && selected == null;
        
        String top = level ? SELECTED.getName() : "";
        String sub = selected != null ? selected.getName() : "";
          
        // Tools without commands
        if(selected != null)
        {
            System.out.println();
            System.out.println("Usage: virge.jar sql " + top + " " + sub + " <OPTIONS>");
            System.out.println();
            System.out.println(selected.getShortDescription());
            System.out.println();
            System.out.println("Options:");
            System.out.println();
            
            print(selected.getHelp(), System.out);
        }
        else if(SELECTED != null)
        {
            // Top level tools
            System.out.println();
            System.out.println("Usage: virge.jar sql " + top + sub + " <COMMAND>");
            System.out.println();
            System.out.println(SELECTED.getShortDescription());
            System.out.println();            
            System.out.println("Options:");
            System.out.println();
            System.out.println(HELP_SPACING + "--help");
            System.out.println(HELP_SPACING + "-h");
            System.out.println(HELP_SPACING + HELP_DESCRIPTION_SPACING + "View a tools options.");
            System.out.println();
            System.out.println("Commands:");
            System.out.println();
            
            print(SELECTED.getHelp(), System.out); 
        }
        else
        {
            // No command
            System.out.println();
            System.out.println("Usage: virge.jar sql <TOOL>");
            System.out.println();
            System.out.println(HELP);
            System.out.println();
            System.out.println("Tools:");
            System.out.println();

            for(Tool help : tools)
            {
                System.out.println(HELP_SPACING + help.getName() + " - " + help.getShortDescription());
            }  

            System.out.println(); 
        }
        
        System.exit(1);
    }
    
    /**
     * Runs the tool, the top level should be trimmed by this point (virge.jar sql drivers) where sql is trimmed.
     * 
     * @param args The command and options to execute.
     * @throws Exception Exceptions raised by tools. 
     */
    public static void main(String[] args) throws Exception
    {
        // NOTE: -? might be a special pattern in some shells, zsh?
        if(args.length == 0 || args[0].equals("--help") || args[0].equals("-h") || args[0].equals("-?"))
        {   
            printToolHelp(null);
         
            return;
        }
        
        SELECTED = lookup.get(args[0]);

        if(SELECTED == null) 
        {
            System.err.println("\nUnknown Command: " + args[0]);
            
            printToolHelp(null);
        }
        
        if(!SELECTED.parse(args, 1)) printToolHelp(null);
        
        SELECTED.execute();
    }

}
