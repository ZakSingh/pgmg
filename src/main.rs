use tokio_postgres::NoTls;
use pgmg::{analyze_statement, filter_builtins, builtin_catalog::BuiltinCatalog};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the database.
    let (client, connection) =
      tokio_postgres::connect("host=localhost user=postgres password=password dbname=postgres", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    
    // Load built-in catalog from the database
    let builtin_catalog = BuiltinCatalog::from_database(&client).await?;

    let sql = "create or replace function api.delete_parcel_template(
    p_template_id int,
    p_account_id  int
) returns void
    language plpgsql
    volatile as
$$
declare
    v_updated_count int;
begin
    update parcel_template
    set deleted_at = now()
    where parcel_template_id = p_template_id
      and account_id = p_account_id;

    get diagnostics v_updated_count = row_count;

    if v_updated_count = 0 then
        raise exception no_data_found using message = 'Parcel template not found or access denied';
    end if;
end;
$$;";


    // Analyze the SQL statement
    let dependencies = analyze_statement(sql)?;
    
    println!("\nRaw dependencies (including built-ins):");
    println!("Relations: {:?}", dependencies.relations);
    println!("Functions: {:?}", dependencies.functions);
    println!("Types: {:?}", dependencies.types);
    
    // Filter out built-ins
    let filtered_deps = filter_builtins(dependencies, &builtin_catalog);
    
    println!("\nFiltered dependencies (excluding built-ins):");
    println!("Relations: {:?}", filtered_deps.relations);
    println!("Functions: {:?}", filtered_deps.functions);
    println!("Types: {:?}", filtered_deps.types);
    
    Ok(())
}

