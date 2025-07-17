use std::collections::HashSet;
use tokio_postgres::Client;
use crate::QualifiedIdent;

/// Catalog of built-in PostgreSQL objects that should be excluded from dependency analysis
#[derive(Debug, Clone)]
pub struct BuiltinCatalog {
    pub functions: HashSet<QualifiedIdent>,
    pub types: HashSet<QualifiedIdent>,
    pub relations: HashSet<QualifiedIdent>,
}

impl BuiltinCatalog {
    /// Create a new empty catalog
    pub fn new() -> Self {
        Self {
            functions: HashSet::new(),
            types: HashSet::new(),
            relations: HashSet::new(),
        }
    }

    /// Load built-in objects from a PostgreSQL database
    pub async fn from_database(client: &Client) -> Result<Self, Box<dyn std::error::Error>> {
        let mut catalog = Self::new();
        
        // Query built-in functions
        catalog.load_builtin_functions(client).await?;
        
        // Query built-in types
        catalog.load_builtin_types(client).await?;
        
        // Query built-in relations (tables and views)
        catalog.load_builtin_relations(client).await?;
        
        Ok(catalog)
    }
    
    async fn load_builtin_functions(&mut self, client: &Client) -> Result<(), Box<dyn std::error::Error>> {
        // Query for built-in functions from pg_catalog
        // We exclude aggregate functions as they're handled separately
        let query = r#"
            SELECT 
                n.nspname as schema_name,
                p.proname as function_name
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname IN ('pg_catalog', 'information_schema')
               OR p.oid < 16384  -- Built-in OIDs are typically < 16384
            GROUP BY n.nspname, p.proname
        "#;
        
        let rows = client.query(query, &[]).await?;
        
        for row in rows {
            let schema: String = row.get(0);
            let name: String = row.get(1);
            
            self.functions.insert(QualifiedIdent::new(Some(schema.clone()), name.clone()));
            // Also add without schema for common usage
            if schema == "pg_catalog" {
                self.functions.insert(QualifiedIdent::from_name(name));
            }
        }
        
        // Add SQL language constructs that aren't in pg_proc but are built-in
        // These are "parser special forms" implemented directly in PostgreSQL's grammar (gram.y)
        // rather than as catalog functions. They cannot be queried from any system catalog.
        // This comprehensive list is necessary for accurate dependency filtering.
        //
        // Note: This list includes only constructs that are truly parser special forms.
        // Regular functions (even built-in ones) that appear in pg_proc are handled by the query above.
        let sql_constructs = [
            // Conditional expressions (SQL standard CASE is different from these)
            "coalesce",
            "nullif", 
            "greatest",
            "least",
            
            // Date/Time special forms (these are keywords, not functions)
            "current_date",
            "current_time", 
            "current_timestamp",
            "localtime",
            "localtimestamp",
            
            // Session information (keywords)
            "current_user",
            "current_role", 
            "session_user",
            "user",
            "current_catalog",
            "current_schema",
            
            // Row and array constructors (special syntax)
            "row",
            "array",
            
            // XML functions with special syntax
            "xmlelement",
            "xmlforest", 
            "xmlpi",
            "xmlroot",
            "xmlexists",
            
            // Grouping sets related
            "grouping",
            
            // Special syntax constructs
            "overlay",  // OVERLAY(string PLACING string FROM int [FOR int])
            "position", // POSITION(substring IN string)
            "substring", // SUBSTRING(string [FROM int] [FOR int])
            "trim",     // TRIM([LEADING | TRAILING | BOTH] [characters] FROM string)
            "extract",  // EXTRACT(field FROM source)
            
            // Type casting (when written as CAST(x AS type))
            "cast",
            
            // Collation specification
            "collation",
            
            // Special value keywords
            "default",
        ];
        
        for construct in sql_constructs {
            self.functions.insert(QualifiedIdent::from_name(construct.to_string()));
        }
        
        Ok(())
    }
    
    async fn load_builtin_types(&mut self, client: &Client) -> Result<(), Box<dyn std::error::Error>> {
        // Query for built-in types
        let query = r#"
            SELECT 
                n.nspname as schema_name,
                t.typname as type_name
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname IN ('pg_catalog', 'information_schema')
               OR t.oid < 16384  -- Built-in OIDs
        "#;
        
        let rows = client.query(query, &[]).await?;
        
        for row in rows {
            let schema: String = row.get(0);
            let name: String = row.get(1);
            
            self.types.insert(QualifiedIdent::new(Some(schema.clone()), name.clone()));
            // Also add without schema for common usage
            if schema == "pg_catalog" {
                self.types.insert(QualifiedIdent::from_name(name));
            }
        }
        
        
        Ok(())
    }
    
    async fn load_builtin_relations(&mut self, client: &Client) -> Result<(), Box<dyn std::error::Error>> {
        // Query for built-in tables and views
        let query = r#"
            SELECT 
                schemaname,
                tablename as relation_name
            FROM pg_tables
            WHERE schemaname IN ('pg_catalog', 'information_schema')
            UNION ALL
            SELECT 
                schemaname,
                viewname as relation_name
            FROM pg_views
            WHERE schemaname IN ('pg_catalog', 'information_schema')
        "#;
        
        let rows = client.query(query, &[]).await?;
        
        for row in rows {
            let schema: String = row.get(0);
            let name: String = row.get(1);
            
            self.relations.insert(QualifiedIdent::new(Some(schema), name));
        }
        
        Ok(())
    }
}

impl Default for BuiltinCatalog {
    fn default() -> Self {
        Self::new()
    }
}