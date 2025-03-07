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
package com.invirgance.virge.sql.generate;

import com.invirgance.convirgance.ConvirganceException;
import com.invirgance.convirgance.input.CSVInput;
import com.invirgance.convirgance.input.DelimitedInput;
import com.invirgance.convirgance.input.Input;
import com.invirgance.convirgance.input.JBINInput;
import com.invirgance.convirgance.input.JSONInput;
import com.invirgance.convirgance.json.JSONObject;
import com.invirgance.convirgance.source.FileSource;
import com.invirgance.convirgance.source.InputStreamSource;
import com.invirgance.convirgance.source.Source;
import com.invirgance.convirgance.transform.CoerceStringsTransformer;
import com.invirgance.virge.Virge;
import static com.invirgance.virge.Virge.exit;
import static com.invirgance.virge.sql.VirgeSQL.HELP_DESCRIPTION_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.HELP_SPACING;
import static com.invirgance.virge.sql.VirgeSQL.printToolHelp;
import com.invirgance.virge.tool.Tool;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.DecimalFormatSymbols;

/**
 *
 * @author jbanes
 */
public class GenerateTable implements Tool
{
    private Source source;
    private Input<JSONObject> input;

    private char inputDelimiter;
    private boolean detectTypes = false;
    private String tableName;

    public Source getSource()
    {
        return source;
    }

    public void setSource(Source source)
    {
        this.source = source;
    }

    public Input<JSONObject> getInput()
    {
        return input;
    }

    public void setInput(Input<JSONObject> input)
    {
        this.input = input;
    }
    
    private boolean isURL(String path)
    {
        char c;
        
        if(!path.contains(":/")) return false;
        if(path.charAt(0) == ':') return false;
        
        for(int i=0; i<path.length(); i++)
        {
            c = path.charAt(i);
            
            if(c == ':') return (path.charAt(i+1) == '/');
                
            if(!Character.isLetter(c)) return false;
        }
        
        return false;
    }
    
    private Source getSource(String path) throws MalformedURLException, IOException
    {
        File file;
        URL url;
        
        if(path.equals("-")) return new InputStreamSource(System.in);

        if(isURL(path))
        {
            url = URI.create(path).toURL();
            
            if(tableName == null)
            {
                tableName = url.getFile();
            
                if(tableName.contains(".")) tableName = tableName.substring(0, tableName.indexOf('.'));
            }

            return new InputStreamSource(url.openStream());
        }
        
        file = new File(path);
        
        if(!file.isFile())
        {
            System.err.println("File not found: " + path);
            
            throw new ConvirganceException("File not found: " + path);
        }
        
        if(tableName == null)
        {
            tableName = file.getName();
            
            if(tableName.contains(".")) tableName = tableName.substring(0, tableName.indexOf('.'));      
        }
        
        return new FileSource(file);
    }
    
    // TODO: Improve auto-detection
    private Input<JSONObject> detectInput(String path) throws MalformedURLException
    {
        if(isURL(path))
        {
            path = URI.create(path).toURL().getFile();
        }
        
        path = path.toLowerCase();
        
        if(path.endsWith(".json")) return new JSONInput();
        if(path.endsWith(".csv")) return new CSVInput(); 
        if(path.endsWith(".jbin")) return new JBINInput();
        
        return null;
    }
    
    @Override
    public String getName()
    {
        return "table";
    }
    
    @Override 
    public String getShortDescription()
    {
        return "Generate a SQL query to create a table based on the source's data.";
    }
    
    @Override
    public String getExample()
    {
        return "virge.jar sql drivers generate table -source <file|url>";
    }    
    
    @Override
    public String[] getHelp()
    {
        return new String[] {
            HELP_SPACING + "--source <file path> or piped data <\"-\">",
            HELP_SPACING +  "-s <file path> or piped data <\"-\">",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Alternate method of specifying the source file",
            "",
            HELP_SPACING + "--source-type <format>",
            HELP_SPACING + "-i <format>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Specify the format of the input file. Currently supported options are json, csv, tsv, pipe, delimited, and jbin",
            "",
            HELP_SPACING + "--source-delimiter <delimiter>",
            HELP_SPACING + "-S <delimiter>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Set the column delimiter if the source is a delimited file (e.g. , or |)",
            "",
            HELP_SPACING + "--name <table name>",
            HELP_SPACING + "-n <table name>",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Specify the name for the generated table. Otherwise the name will be created from the 'source'",
            "",
            HELP_SPACING + "--detect-input-types",
            HELP_SPACING + "-auto",
            HELP_SPACING + HELP_DESCRIPTION_SPACING + "Detect the actual datatypes from the source file ex \"5\" would turn into an interger",
            "",           
            HELP_SPACING + "--help",
            HELP_SPACING + "-h",
            HELP_SPACING + "-?",
            HELP_SPACING + HELP_DESCRIPTION_SPACING  + "Display this menu.",     
        };
    }

    private boolean error(String message)
    {
        System.err.println(message);
        
        return false;
    }
    
    private Input<JSONObject> getInputType(String type)
    {
        switch(type)
        {
            case "csv": 
                return new CSVInput();
            
            case "tsv":
                return new DelimitedInput('\t');
            
            case "pipe":
            case "|":
                return new DelimitedInput('|');
            
            case "delimited":
                
                if(inputDelimiter != 0) return new DelimitedInput(inputDelimiter);
                
                return new DelimitedInput();
            
            case "jbin":
                return new JBINInput();
                
            case "json":
                return new JSONInput();
                
            default:
                exit(255, "Unknown input type: " + type);
                return null; // Keep the compiler happy
        }
    }
    
    @Override
    public boolean parse(String[] args, int start) throws Exception
    {
        
        if(args.length == start) return false;
        
        for(int i=start; i<args.length; i++)
        {
            // Handle single-letter params with no spaces in them
            if(args[i].length() > 2 && args[i].charAt(0) == '-' && Character.isLetterOrDigit(args[i].charAt(1)))
            {
                parse(new String[]{ args[i].substring(0, 2), args[i].substring(2) }, 0);
                
                continue;
            }
            
            switch(args[i])
            {
                case "--help":
                case "-h":
                case "-?":
                    printToolHelp(this);
                    return true;
                
                case "--source-delimiter":
                case "-S":
                    inputDelimiter = args[++i].charAt(0);
                    
                    if(input instanceof DelimitedInput) ((DelimitedInput)input).setDelimiter(inputDelimiter);
                    
                    break;
                    
                case "--source-type":
                case "-i":
                    input = getInputType(args[++i]);
                    
                    if(input instanceof DelimitedInput) ((DelimitedInput)input).setDelimiter(inputDelimiter);
                    
                    break;
                    
                case "--detect-input-types":
                case "-auto":
                    detectTypes = true;
                    break;        
                    
                case "--source":
                case "-s":
                    source = getSource(args[++i]);
                    
                    if(input == null) input = detectInput(args[i]);
                    
                    break;
                    
                case "--name":
                case "-n":
                    tableName = args[++i];
                    break;
                    
                default:
                    
                    if(source == null && (args[i].equals("-") || args[i].contains(".")))
                    {
                        source = getSource(args[i]);
                    
                        if(input == null) input = detectInput(args[i]);

                        break;
                    }
                    else
                    {
                        exit(255, "Unknown parameter: " + args[i]);
                    }
            }
        }
        
        if(tableName == null) return error("No table name specified, and cannot be inferred from source! Use -n to specify a name.");
        if(source == null) return error("No source specified!");
        if(input == null) return error("No input type specified and unable to autodetect");
        
        return true;
    }

    private String normalizeObjectName(String name)
    {
        return "\"" + name + "\"";
    }
    
    /**
     * Returns a string that can be used to create a table based on the source data.
     * 
     * @param source The file source
     * @param input The input source type (CSVInput, JSONInput, etc...)
     * @param name The table name
     * @param detect If types should be inferred from the source files values
     * @return A String representing a SQL create statement.
     * @throws Exception If something goes horribly wrong.
     */
    public String generateTableSQL(Source source, Input<JSONObject> input, String name, boolean detect) throws Exception
    {
       Iterable<JSONObject> iterable;
       StringBuffer sql = new StringBuffer();
       StringBuffer comments = new StringBuffer();

       Column[] columns = null;
       int index;

       if(name == null) Virge.exit(254, "No table name specified! Use -n to specify a name.");
       if(source == null) Virge.exit(254, "No source specified!");
       if(input == null) Virge.exit(254, "No input type specified and unable to autodetect");

       iterable = input.read(source);

       if(detect) iterable = new CoerceStringsTransformer().transform(iterable);

       for(JSONObject record : iterable)
       {
           if(columns == null) 
           {
               columns = new Column[record.size()];
               index = 0;

               for(String key : record.keySet())
               {
                   columns[index++] = new Column(key);
               }
           }
           
           for(Column column : columns)
           {
               column.analyze(record.get(column.name));
           }   
       }

       sql.append("CREATE TABLE ");
       sql.append(name);
       sql.append(" (\n");

       index = 0;

       for(Column column : columns)
       {
           if(index++ > 0) sql.append(",\n");

           sql.append("    ");
           sql.append(normalizeObjectName(column.name));
           sql.append(" ");
           sql.append(column.getType());

           if(column.nullable)
           {
               sql.append(" ");
               sql.append("NULL");
           }
       }

       sql.append("\n);\n");

       return comments.toString() + sql.toString();
    }   
    
    @Override
    public void execute() throws Exception
    {
        System.out.println(generateTableSQL(source, input, tableName, detectTypes));
    }
    
    private class Column
    {
        String name;
        
        DecimalFormatSymbols symbols;
        
        Boolean numeric;
        Boolean decimal;
        Boolean bool;
        boolean nullable;
        
        long smallestInteger;
        long largestInteger;
        
        double smallestDouble;
        double largestDouble;
        
        int length = 8; // Minimum 8 character varchar

        public Column(String name)
        {
            this.name = name;
            this.symbols = new DecimalFormatSymbols();
        }
        
        private boolean isNumeric(Object value)
        {
            String string;
            int periods = 0;
            
            char point = symbols.getDecimalSeparator();
            char c;
            
            if(value instanceof Number) return true;
            
//            if(value instanceof String)
//            {
//                string = value.toString();
//                
//                for(int i=0; i<string.length(); i++)
//                {
//                    c = string.charAt(i);
//                    
//                    if(!Character.isDigit(c) && c != point && !(c == '-' && i == 0))
//                    {
//                        return false;
//                    }
//                    
//                    if(c == point) periods++;
//                }
//                
//                return (periods <= 1);
//            }
            
            return false;
        }
        
        private boolean isDecimal(Object value)
        {
            char point = symbols.getDecimalSeparator();
            
            if(!numeric) return false;
            
            if(value instanceof Float) return true;
            if(value instanceof Double) return true;
            if(value instanceof BigDecimal) return true;
            
//            if(value instanceof String && value.toString().indexOf(point) > 0) return true;
            
            return false;
        }
        
        private boolean isBoolean(Object value)
        {
            if(decimal) return false;
            
            if(value instanceof Boolean) return true;
            if(numeric && value.toString().equals("1")) return true;
            if(numeric && value.toString().equals("0")) return true;
            
            if(value instanceof String)
            {
                if(value.toString().equalsIgnoreCase("true")) return true;
                if(value.toString().equalsIgnoreCase("false")) return true;
            }
            
            return false;
        }
        
        // TODO: bug, what if I want my numbers as string?
        // temporary mitigation: modified isNumeric and isDecimal, and using coerceStringsTransformer in the calling function
        public void analyze(Object value)
        {
            long tempLong;
            double tempDouble;
            byte[] tempBytes;
            
            if(value == null || value.equals(""))
            {
                nullable = true;
                return;
            }
            
            if(numeric == null || numeric) numeric = isNumeric(value);
            if(decimal == null || !decimal) decimal = isDecimal(value);
            if(bool == null || bool) bool = isBoolean(value);
            
            if(numeric && !decimal)
            {
                tempLong = Long.parseLong(value.toString());
                
                if(tempLong < smallestInteger) smallestInteger = tempLong;
                if(tempLong > smallestInteger) largestInteger = tempLong;
            }
            
            if(numeric && decimal)
            {
                tempDouble = Double.parseDouble(value.toString());
                
                if(tempDouble < smallestDouble) smallestDouble = tempDouble;
                if(tempDouble > largestDouble) largestDouble = tempDouble;
            }
            
            tempBytes = value.toString().getBytes();
            
            if(tempBytes.length > length) length = tempBytes.length;
        }
        
        private int getPrecision()
        {
            int length = Math.max(Double.toString(smallestDouble).length(), Double.toString(largestDouble).length());
            
            return Math.max(Math.min(38, length), 12);
        }
        
        private int getScale()
        {
            long scale = (long)Math.max(Math.abs(smallestDouble), Math.abs(largestDouble));

            return Math.min(getPrecision() - Long.toString(scale).length(), 8);
        }
        
        private String getIntegerType()
        {
            if(smallestDouble <= Integer.MIN_VALUE) return "BIGINT";
            if(largestDouble >= Integer.MAX_VALUE) return "BIGINT";
            
            return "INT";
        }
        
        private String getVarchar()
        {
            if(length > 2048) return "CLOB";
            
            for(int i=8; i<=2048; i*=2)
            {
                if(i >= length) return "VARCHAR(" + i + ")";
            }
            
            return "VARCHAR";
        }
        
        public String getType()
        {
            if(bool != null && bool) return "BIT";
            if(numeric != null && numeric && decimal) return "NUMERIC(" + getPrecision() + "," + getScale() + ")";
            if(numeric != null && numeric) return getIntegerType();
            
            return getVarchar();
        }
    }
}
