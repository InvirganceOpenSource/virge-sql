/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.invirgance.virge.sql;

import static com.invirgance.virge.Virge.HELP_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.sql.connections.CreateStoredConnection;
import com.invirgance.virge.tool.Tool;
import java.util.ArrayList;

/**
 * For creating StoredConnections and configuring DataSources.
 * @author tadghh
 */
public class ConnectionTools implements Tool
{
    private static final Tool[] TOOLS = new Tool[]{
        new CreateStoredConnection(),
    };

    private Tool tool;

    @Override
    public String getName()
    {
        return "connection";
    }
    
    @Override
    public String getShortDescription()
    {
        return "Configure StoredConnections and DataSources.";
    }
    
    @Override
    public String[] getHelp()
    {
        ArrayList<String> help = new ArrayList<>();
        
        for(Tool tool : TOOLS)
        {
            help.add(HELP_SPACING + tool.getName() + " - " + tool.getShortDescription());
        }
        
        return help.toArray(new String[0]);
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
                    
                    printToolHelp(this.tool);
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
