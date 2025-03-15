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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Prints out provided data into named columns.
 * Column width is determined by the title or the widest item.
 * 
 * Example:
 *      // names and examples are Lists of String(s)
 *      AutomaticDrivers drivers = new AutomaticDrivers();
 *       
 *      for(AutomaticDriver descriptor : drivers)
 *      {
 *           example = !(descriptor.getExamples().length == 0) ? descriptor.getExamples()[0] : "";
 *           
 *           examples.add(example);
 *           names.add(descriptor.getName());
 *      }
 *       
 *      new PrintInfo()
 *               .addColumn("Database Name", names)
 *               .addColumn("Connection String Example", examples)
 *               .print();
 * 
 * 
 * @author tadghh
 */
public class ConsoleOutputFormatter
{
    private final List<String> titles = new ArrayList<>();
    private final List<List<String>> columns = new ArrayList<>();
    
    /**
     * Adds a titled column with its data to the output.
     * 
     * @param title The column title
     * @param data The data for the column
     * @return This ConsoleOutputFormatter
     */
    public ConsoleOutputFormatter addColumn(String title, List<String> data)
    {
        List<String> column = new ArrayList<>();
        
        titles.add(title);
       
        for(String item : data) column.add(item);
        
        columns.add(column);
        
        return this;
    }
    
    /**
     * Adds a titled column with its data to the output.
     * 
     * @param title The column title
     * @param data The data for the column
     * @return This ConsoleOutputFormatter
     */
    public ConsoleOutputFormatter addColumn(String title, String[] data) 
    {
        List<String> dataList = Arrays.asList(data);
        
        return addColumn(title, dataList);
    }
   
    /**
     * Prints the formatted columns to standard out.
     */
    public void print() 
    {
        int maxRows = getDataRows();
        int[] widths = calculateColumnWidths();
        
        List<String> rowData = new ArrayList<>();
        
        printRow(titles, widths);
        printSeparator(widths);
        
        for(int row = 0; row < maxRows; row++)
        {
            rowData.clear();
            
            for(List<String> column : columns)
            {
                rowData.add(row < column.size() ? column.get(row) : "");
            }
            
            printRow(rowData, widths);
        }
    }
    
    private int[] calculateColumnWidths() 
    {
        List<String> column;
        int[] widths = new int[titles.size()];
        
        for(int i = 0; i < titles.size(); i++) 
        {
            widths[i] = titles.get(i).length();
        }
        
        for(int i = 0; i < columns.size(); i++) 
        {
            column = columns.get(i);
            
            for(String value : column) 
            {
                if(widths[i] < value.length()) widths[i] = value.length();
            }
        }
        
        return widths;
    }
    
    private void printRow(List<String> rowData, int[] widths) 
    {
        for(int i = 0; i < rowData.size(); i++) 
        {
            System.out.print(formatWidth(rowData.get(i), widths[i]));
            
            if(i < rowData.size() - 1) System.out.print("  ");
        }
        
        System.out.println();
    }

    private void printSeparator(int[] widths) 
    {
        for(int i = 0; i < widths.length; i++) 
        {
            System.out.print(drawWidth('=', widths[i]));
            
            if(i < widths.length - 1) System.out.print("  ");
        }
        
        System.out.println();
    }
    
    private String formatWidth(String value, int width) 
    {
        StringBuilder builder = new StringBuilder(value);
        
        while(builder.length() < width) builder.append(" ");
        
        return builder.toString();
    }
    
    private String drawWidth(char c, int width) 
    {
        StringBuilder buffer = new StringBuilder();
       
        while(buffer.length() < width) buffer.append(c);
        
        return buffer.toString();
    }
    
    private int getDataRows() 
    {
        int max = 0;
        
        for(List<String> column : columns) max = Math.max(max, column.size());
        
        return max;
    }
}
