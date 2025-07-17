use pg_query::parse_plpgsql;
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

    let x = parse_plpgsql("create or replace function api.update_items(
    p_account_id int,
    p_items      api.update_item[]
) returns int[]
    language plpgsql
    volatile
as $$
declare
    v_item             api.update_item;
    v_updated_items    int[] := array []::int[];
    v_current_quantity int;
    v_quantity_delta   int;
    v_image_path       text;
    v_order            int;
begin
    -- Validate ownership of all items first
    foreach v_item in array p_items
    loop
        if not exists (
            select 1
            from item
            where item_id = v_item.item_id
              and account_id = p_account_id) then
            raise exception 'Item % not found or access denied', v_item.item_id using errcode = 'M0005';
        end if;
    end loop;

    -- Process each item update
    foreach v_item in array p_items
    loop
        -- Update item fields if provided
        update item
        set mini_content_status  = coalesce(v_item.mini_content_status,
                                            mini_content_status),
            mini_assembly_status = coalesce(v_item.mini_assembly_status,
                                            mini_assembly_status),
            mini_painting_status = coalesce(v_item.mini_painting_status,
                                            mini_painting_status),
            mini_color           = coalesce(v_item.mini_color, mini_color),
            description          = case
                when v_item.description is not null then nullif(v_item.description, '')
                else description
                                   end,
            archived_at          = case when v_item.is_hidden is true and item.archived_at is null
                                            then now()
                                   end,
            updated_at           = now()
        where item_id = v_item.item_id;

        select l.quantity_available
        from views.listing l
        where item_id = v_item.item_id
        into v_current_quantity;

        -- if ONLY false provided, no listing modifications need to be made except for

        if v_current_quantity is not null then
            -- The item already has an existing listing. Modify it instead of creating one.

            -- Update listing fields
            update listing
            set price           = coalesce((v_item.listing).price, price),
                length          = coalesce(
                        case when null_if_empty((v_item.listing).parcel_size) is not null
                             then (null_if_empty((v_item.listing).parcel_size)).length
                             else null end,
                        length),
                width           = coalesce(
                        case when null_if_empty((v_item.listing).parcel_size) is not null
                             then (null_if_empty((v_item.listing).parcel_size)).width
                             else null end,
                        width),
                height          = coalesce(
                        case when null_if_empty((v_item.listing).parcel_size) is not null
                             then (null_if_empty((v_item.listing).parcel_size)).height
                             else null end,
                        height),
                dimensions_unit = coalesce(
                        case when null_if_empty((v_item.listing).parcel_size) is not null
                             then (null_if_empty((v_item.listing).parcel_size)).unit
                             else null end,
                        dimensions_unit),
                weight          = coalesce(
                        case when (v_item.listing).weight is not null
                             then ((v_item.listing).weight).weight
                             else null end,
                        weight),
                weight_unit     = coalesce(
                        case when (v_item.listing).weight is not null
                             then ((v_item.listing).weight).unit::text::mass_unit
                             else null end,
                        weight_unit),
                is_enabled      = coalesce((v_item.listing).is_enabled, is_enabled),
                updated_at      = now()
            where item_id = v_item.item_id;

            if (v_item.listing).quantity is not null then
                -- Handle quantity update with inventory adjustment (delta = new quantity - old quantity)
                v_quantity_delta := (v_item.listing).quantity - v_current_quantity;

                -- Create inventory adjustment if quantity changed
                if v_quantity_delta != 0 then
                    insert into inventory_adjustment (item_id, delta)
                    values (v_item.item_id, v_quantity_delta);
                end if;
            end if;

        elsif (v_item.listing).is_enabled = true then
            -- The item doesn't have a listing yet. Create one if the necessary fields are provided.
            insert into listing (
                item_id, initial_quantity, price, length, width, height, dimensions_unit, weight, weight_unit)
            values (
                       v_item.item_id,
                       (v_item.listing).quantity,
                       (v_item.listing).price,
                       ((v_item.listing).parcel_size).length,
                       ((v_item.listing).parcel_size).width,
                       ((v_item.listing).parcel_size).height,
                       ((v_item.listing).parcel_size).unit,
                       ((v_item.listing).weight).weight,
                       ((v_item.listing).weight).unit::text::mass_unit);
        end if;

        -- Handle image updates if provided
        if v_item.images is not null then
            -- hack: assign temporary high display_order values to avoid conflicts
            -- This ensures we can reorder without violating the constraints on item_image
            update item_image
            set display_order = display_order + 10000
            where item_id = v_item.item_id
              and deleted_at is null;

            -- Process the new images array
            v_order := 0;
            foreach v_image_path in array v_item.images
            loop
                -- Try to restore existing image (preserves original created_at)
                update item_image
                set display_order = v_order,
                    deleted_at    = null
                where item_id = v_item.item_id
                  and path = v_image_path;

                -- Insert new image if it doesn't exist
                if not found then
                    insert into item_image (item_id, path, display_order)
                    values (v_item.item_id, v_image_path, v_order);
                end if;

                v_order := v_order + 1;
            end loop;

            -- Now safe to delete images not in the new array (trigger ensures at least one remains)
            update item_image
            set deleted_at = now()
            where item_id = v_item.item_id
              and deleted_at is null
              and display_order >= 10000;
        end if;

        v_updated_items := array_append(v_updated_items, v_item.item_id);
    end loop;

    return v_updated_items;
end;
$$;").unwrap();

    println!("{}", x);
    let dependencies = analyze_statement(sql)?;
    
    Ok(())
}

