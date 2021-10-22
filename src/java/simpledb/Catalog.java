package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

    public static class Table {
        private int tableID;
        private DbFile dbFile;    // contents of the table
        private String name;    // the name of the table, may be an empty string
        private String pkeyField;   // the name of the primary key

        public Table(DbFile dbFile, String name, String pkeyField) {
            this.tableID = dbFile.getId();
            this.dbFile = dbFile;
            this.name = name;
            this.pkeyField = pkeyField;
        }

        public int getTableID() {
            return tableID;
        }

        public DbFile getDbFile() {
            return dbFile;
        }

        public String getName() {
            return name;
        }

        public String getPkeyField() {
            return pkeyField;
        }

    }

    private Map<String, Table> nameTableMap;    // key: name of table; val: table instance
    private Map<Integer, Table> idTableMap; // key: table id; val: table instance

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
        this.nameTableMap = new HashMap<>();
        this.idTableMap = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
        // some code goes here
        Table t = new Table(file, name, pkeyField);
        this.idTableMap.put(t.getTableID(), t);
        if(name != null && name != "") {
            this.nameTableMap.put(t.getName(), t);
        }
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        // some code goes here
        Table t = this.nameTableMap.get(name);
        if(t == null) {
            throw new NoSuchElementException("do not have table name: " + name);
        }
        return t.getTableID();
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableId The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableId) throws NoSuchElementException {
        // some code goes here
        Table t = this.idTableMap.get(tableId);
        if(t == null) {
            throw new NoSuchElementException("do not have tableId: " + tableId);
        }
        return t.dbFile.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableId The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableId) throws NoSuchElementException {
        // some code goes here
        Table t = this.idTableMap.get(tableId);
        if(t == null) {
            throw new NoSuchElementException("do not have tableId: " + tableId);
        }
        return t.dbFile;
    }

    public String getPrimaryKey(int tableId) {
        // some code goes here
        Table t = this.idTableMap.get(tableId);
        if(t == null) {
            return null;
        }
        return t.getPkeyField();
    }

    public Iterator<Integer> tableIdIterator() {
        // some code goes here
        return this.idTableMap.keySet().iterator();
    }

    public String getTableName(int id) {
        // some code goes here
        Table t = this.idTableMap.get(id);
        if(t == null) {
            return null;
        }
        return t.getName();
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
        // some code goes here
        this.idTableMap.clear();
        this.nameTableMap.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

