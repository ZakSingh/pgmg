use tokio_postgres::NoTls;
use pgmg::analyze_statement;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connect to the database.
    let (_client, connection) =
      tokio_postgres::connect("host=localhost user=postgres password=password dbname=miniswap", NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    let sql = "select (
        coalesce(sum(convert_currency(id.price, p_currency_code) * cl.quantity),
                 (0, p_currency_code)::currency),
        coalesce(sum(cl.quantity), 0)::int
           )::api.cart_summary
from cart_listing cl
     join api.item_details id on cl.item_id = id.item_id
where cl.account_id = p_account_id
  and cl.selected_for_checkout = true;";
  
    let dependencies = analyze_statement(sql)?;
    
    println!("SQL Statement Dependencies:");
    println!("=========================");
    
    println!("\nRelations:");
    for relation in &dependencies.relations {
        match &relation.schema {
            Some(schema) => println!("  {}.{}", schema, relation.name),
            None => println!("  {}", relation.name),
        }
    }
    
    println!("\nFunctions:");
    for function in &dependencies.functions {
        match &function.schema {
            Some(schema) => println!("  {}.{}", schema, function.name),
            None => println!("  {}", function.name),
        }
    }
    
    println!("\nTypes:");
    for type_ref in &dependencies.types {
        match &type_ref.schema {
            Some(schema) => println!("  {}.{}", schema, type_ref.name),
            None => println!("  {}", type_ref.name),
        }
    }

    Ok(())
}

