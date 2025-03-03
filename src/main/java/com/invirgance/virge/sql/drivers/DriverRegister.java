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

import com.invirgance.convirgance.json.JSONArray;
import com.invirgance.convirgance.json.JSONObject;
import static com.invirgance.virge.Virge.printHelp;
import com.invirgance.virge.jdbc.JDBCDrivers;
import com.invirgance.virge.tool.Tool;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author tadghh
 */
public class DriverRegister implements Tool
{
    private String driver;
    private String name;
    private String datasource;
 
    private final List<String> artifact = new ArrayList<>();
    private final List<String> prefix = new ArrayList<>();
    private final List<String> shortName = new ArrayList<>();
    private final List<String> example = new ArrayList<>();
    
    @Override
    public String getName()
    {
        return "register";
    }

    @Override
    public String[] getHelp()
    {
        return new String[]{
            "    register",
            "        --name <name>",
            "        -n <name>",
            "            Set the name of the driver. If the name matches an existing",
            "            driver, the existing driver will be updated.",
            "",
            "        --artifact <groupId:artifactId:version>",
            "        -a <groupId:artifactId:version>",
            "            The Maven coordinates of the JDBC driver. This option can be",
            "            specified more than once if multiple JARs are needed.",
            "",
            "        --driver <className>",
            "        -d <className>",
            "            The class name of the JDBC Driver implementation.",
            "",
            "        --data-source <className>",
            "        -D <className>",
            "            The class name of the JDBC DataSource implementation. If",
            "            not specified, a default Data Source wrapping the Driver",
            "            will be used.",
            "",
            "        --prefix <url prefix>",
            "        -p <url prefix>",
            "            The url prefix used by this driver. e.g. jdbc:oracle:",
            "            This option can be specified more than once if multiple",
            "            prefixes are supported.",
            "",
            "        --short-name <name>",
            "        -s <name>",
            "            Add a short name for this driver",
        };
    }

    @Override
    public boolean parse(String[] args, int start) throws Exception
    {
        if(start == args.length)
        {
            printHelp(this);   
            
            return true;
        }
        
        for(int i=start; i<args.length; i++)
        {            
            switch(args[i])
            {
                case "--name":
                case "-n":
                    this.name = args[++i];
                    break;
                
                case "--artifact":
                case "-a":
                    this.artifact.add(args[++i]);
                    break;                    
                
                case "--driver":
                case "-d":
                    this.driver = args[++i];
                    break;
                    
                case "--data-source":
                case "-D":
                    this.datasource = args[++i];
                    break;
                    
                case "--prefix":
                case "-p":
                    this.prefix.add(args[++i]);
                    break;
                // TODO remove this  
                case "--short-name":
                case "-k":
                    this.shortName.add(args[++i]);
                    break;
                
                case "--help":
                case "-h":
                    printHelp(this);    
                    break;   
                    
                default:
                    System.out.println("Unknown parameter: " + args[start]);
                    printHelp(this);
            }
        }
        
        return true;
    }

    @Override
    public void execute() throws Exception
    {
        registerDriver();    
    }
    
    public void registerDriver()
    {
        JDBCDrivers drivers = new JDBCDrivers();
        JSONObject descriptor = drivers.getDescriptor(name);
        
        if(descriptor == null) 
        {
            descriptor = new JSONObject(true);
            
            descriptor.put("name", name);
            descriptor.put("keys", new JSONArray());
            descriptor.put("artifact", new JSONArray());
            descriptor.put("driver", "");
            // TODO maybe a problem? Do we need to shrinkwrap back over to virge?
            descriptor.put("datasource", "com.invirgance.virge.jdbc.DriverDataSource");
            descriptor.put("prefixes", new JSONArray());
            descriptor.put("examples", new JSONArray());
        }
        
        descriptor.put("name", name);
        
        if(driver != null) descriptor.put("driver", driver);
        if(datasource != null) descriptor.put("datasource", datasource);
        
        add(descriptor.getJSONArray("keys"), shortName);
        add(descriptor.getJSONArray("artifact"), artifact);
        add(descriptor.getJSONArray("prefixes"), prefix);
        add(descriptor.getJSONArray("examples"), example);
        
        if(descriptor.get("name") == null || descriptor.getString("name").length() < 1)
        {
            System.err.println("Unique name is required!");
            System.exit(1);
        }
        
        if(descriptor.get("driver") == null || descriptor.getString("driver").length() < 1)
        {
            System.err.println("Driver class is required!");
            System.exit(1);
        }
        
        if(descriptor.getJSONArray("artifact").size() < 1)
        {
            System.err.println("Maven artifact is required!");
            System.exit(1);
        }
        
        if(descriptor.getJSONArray("prefixes").size() < 1)
        {
            System.err.println("JDBC URL prefix is required to identify driver URLs!");
            System.exit(1);
        }
        
        drivers.addDescriptor(descriptor);
        
        System.err.println("Registered: " + driver);
        System.out.println(descriptor.toString(4));
    }
    
    private void add(JSONArray<String> array, List<String> addition)
    {
        for(String item : addition)
        {
            if(!array.contains(item)) array.add(item);
        }
    }
}
