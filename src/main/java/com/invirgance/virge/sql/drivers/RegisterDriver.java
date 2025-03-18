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
import static com.invirgance.virge.Virge.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.Virge.HELP_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;
import java.util.ArrayList;
import java.util.List;

/**
 * Virge CLI tool, registers a custom driver to use with stored connections or with other Virge SQL tools.
 * 
 * @author tadghh
 */
public class RegisterDriver implements Tool
{
    private String driver;
    private String name;
    private String datasource;
 
    private final List<String> artifacts = new ArrayList<>();
    private final List<String> prefixes = new ArrayList<>();
    private final List<String> examples = new ArrayList<>();
    
    @Override
    public String getName()
    {
        return "add";
    }

    @Override
    public String[] getHelp()
    {
        return new String[]
        {
            HELP_SPACING + "--name <NAME>",
            HELP_SPACING + "-n <NAME>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Set the name of the driver. If the name matches an existing",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "driver, the existing driver will be updated.",
            "",
            HELP_SPACING + "--artifact <MAVEN_COORDINATES>",
            HELP_SPACING + "-a <MAVEN_COORDINATES>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "The Maven coordinates of the JDBC driver. This option can be",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "specified more than once if multiple JARs are needed.",
            "",
            HELP_SPACING + "--driver <CANONICAL_NAME>",
            HELP_SPACING + "-d <CANONICAL_NAME>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "The Canonical name of the JDBC Driver implementation.",
            "",
            HELP_SPACING + "--data-source [DATA_SOURCE]",
            HELP_SPACING + "-D [DATA_SOURCE]",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "The class name of the JDBC DataSource implementation. If",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "not specified, a default Data Source wrapping the Driver",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "will be used.",
            "",
            HELP_SPACING + "--prefix <URL_PREFIX>",
            HELP_SPACING + "-p <URL_PREFIX>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "The url prefix used by this driver. e.g. jdbc:oracle:",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "This option can be specified more than once if multiple",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "prefixes are supported by the driver.",
            "",
            HELP_SPACING + "--example [URL_EXAMPLE]",
            HELP_SPACING + "-e [URL_EXAMPLE]",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "An example connection string to display when viewing information about the driver.",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "This option can be specified more than once.",
            "",           
            HELP_SPACING + "--help",
            HELP_SPACING + "-h",
            HELP_SPACING + HELP_DESCRIPTION_SPACING  + "Display this menu.",     
        };
    }
    
    @Override 
    public String getShortDescription()
    {
        return "Add a new database driver for creating connections.";
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
                
                case "--example":
                case "-e":
                    this.examples.add(args[++i]);
                    break;                 
                    
                case "--artifact":
                case "-a":
                    this.artifacts.add(args[++i]);
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
                    this.prefixes.add(args[++i]);
                    break;
                
                case "--help":
                case "-h":
                    printToolHelp(this);      
                    break;   
                    
                default:
                    System.err.println("Unknown option: " + args[i]);
                    return false;
            }
        }
        
        return true;
    }

    @Override
    public void execute() throws Exception
    {
        registerDriver();    
    }
   
    @Override
    public String getExample()
    {
        return "virge.jar sql drivers add -n <driver name> -p <url:prefix> -a <artifact:ID> -d <org.example.jdbc.ClientDriver>";
    }
    
    /**
     * Registers the driver, if a driver with an equal name already exists its information will be replaced.
     */
    public void registerDriver()
    {
        AutomaticDrivers.AutomaticDriverBuilder builder;
        AutomaticDrivers drivers = new AutomaticDrivers();
        AutomaticDriver descriptor = drivers.getDriverByName(name);
        
        if(descriptor == null) 
        {

            builder = drivers.createDriver(name)
                    .artifact(artifacts.toArray(new String[artifacts.size()]))
                    .prefix(prefixes.toArray(new String[prefixes.size()]))
                    .example(examples.toArray(new String[examples.size()]));
            
            if(driver != null) builder = builder.driver(driver);
            
            if(datasource != null)
            {
                builder = builder.datasource(datasource);
            }
            else
            {
                builder = builder.datasource("com.invirgance.virge.jdbc.DriverDataSource");
            }
            
            descriptor = builder.build();
            
            System.err.println("Registered new Driver: " + descriptor.getName());
        }
        else
        {
            // Updating information for an existing driver
            if(!artifacts.isEmpty()) descriptor.setArtifacts(artifacts.toArray(new String[artifacts.size()]));
            if(!prefixes.isEmpty()) descriptor.setPrefixes(prefixes.toArray(new String[prefixes.size()]));
            if(!examples.isEmpty()) descriptor.setExamples(examples.toArray(new String[examples.size()]));
                        
            if(driver != null) descriptor.setDriver(driver);
            if(datasource != null) descriptor.setDataSource(datasource);
            
            System.out.println("Updated existing driver: " + name);
        }
        
        if(descriptor.getName() == null || descriptor.getName().length() < 1)
        {
            System.err.println("Unique name is required!");
            System.out.println("Hint: use -n to specify a simple name to use when working with the driver.");

            System.exit(1);
        }
        
        if(descriptor.getDriver() == null)
        {
            System.err.println("Driver class is required!");
            System.out.println("Hint: use -d to specify the driver class, double check that the 'd' is lowercase.");

            System.exit(1);
        }
        
        if(descriptor.getArtifacts().length < 1)
        {
            System.err.println("Maven artifact is required!");
            System.out.println("Hint: use -a to specify the artifact.");
 
            System.exit(1);
        }
        
        if(descriptor.getPrefixes().length < 1)
        {
            System.err.println("JDBC URL prefix is required to identify driver URLs!");
            System.out.println("Hint: use -p to specify the prefix.");
 
            System.exit(1);
        }
        
        descriptor.save();

        System.out.println("Saved!");
        System.out.println(descriptor.toString());
    }
    
}
