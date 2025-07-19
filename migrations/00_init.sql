create type country_code as enum ( 'us', 'ca', 'gb' );

create type distance_unit as enum ('mm', 'cm', 'm', 'in', 'ft');

create domain email as citext constraint email_not_empty check (trim(value) <> '');

create domain url as text;

create domain verification_code as text constraint verification_code_format_valid check (value ~ '^\d{6}$');

create domain phone as citext constraint phone_not_empty check (trim(value) <> '');


create type address_inner as
(
    line1       text,
    line2       text,
    city        text,
    state       text,
    postal_code text,
    country     country_code
);

create domain address as address_inner
    constraint line1_required
        check (value is null or ((value).line1 is not null and trim((value).line1) <> ''))
    constraint city_required
        check (value is null or ((value).city is not null and trim((value).city) <> ''))
    constraint postal_code_required
        check (value is null or
               ((value).postal_code is not null and trim((value).postal_code) <> ''))
    constraint country_required
        check (value is null or ((value).country is not null));

comment on column address_inner.line1 is '@pgrpc_not_null';

comment on column address_inner.city is '@pgrpc_not_null';

comment on column address_inner.state is '@pgrpc_not_null';

comment on column address_inner.postal_code is '@pgrpc_not_null';

comment on column address_inner.country is '@pgrpc_not_null';

create type _contact as (
    name    text,
    phone   phone,
    address address
);

comment on column _contact.name is '@pgrpc_not_null';

comment on column _contact.address is '@pgrpc_not_null';

create domain contact as _contact
    constraint name_not_empty check (value is null or ((value).name is not null and
                                                       trim((value).name) <> ''));

create or replace function convert_distance_to_mm(
    distance NUMERIC(10, 2),
    unit     distance_unit
) returns NUMERIC(10, 2)
    language sql
    immutable strict
as $$
select case unit
           when 'mm' then distance
           when 'cm' then distance * 10
           when 'm'  then distance * 1000
           when 'in' then distance * 25.4
           when 'ft' then distance * 304.8
       end;
$$;

create table stripe_event (
    event_id   text primary key,
    event_type text        not null,
    object_id  text,
    created_at timestamptz not null default now()
);

create type shippo_event_type as enum (
    'transaction_created',
    'transaction_updated',
    'track_updated'
    );

create table shippo_event (
    event_type shippo_event_type not null,
    object_id  text              not null,
    event_ts   timestamptz       not null,

    primary key (event_type, object_id, event_ts)
);


-- Utility functions

create or replace function null_if_empty(
    t anyelement
) returns anyelement
    language sql
    immutable parallel safe as
$$
select case when (t.*) is null then null else t end;
$$;

comment on function null_if_empty(t anyelement) is $$
    Given a composite type, return `null` if all of its fields are
    null. Otherwise return the type unchanged.
$$;

create or replace function array_comp_sfunc(
    state anyarray,
    value anyelement
) returns anyarray
    language plpgsql
    immutable parallel safe as
$$
begin
    if value is null or (value.* is null) then
        return state;
    else
        return state || value;
    end if;
end;
$$;

create aggregate array_agg_comp(anyelement) ( sfunc = array_comp_sfunc, stype = anyarray, initcond = '{}', parallel = safe );

comment on aggregate array_agg_comp(anyelement) is $$
    Used to embed arrays of composite types.
    This function will aggregate a composite type into an array of composite types,
    replacing any empty composite type rows (i.e. `(,,,)`) with `null`.

    In almost every situation, we'd rather have an empty array returned instead of a null array.
$$;

create domain slug as citext constraint is_valid_slug check (value ~ '^[a-z0-9]([a-z0-9\\-])+[a-z0-9]$');

create or replace function slugify(
    "value" text
)
    returns slug
    language plpgsql
    strict immutable as
$$
begin
    return trim('-' from
                regexp_replace(
                        replace(lower(unaccent("value")), '''', ''),
                        '[^a-z0-9\\-_]+', '-',
                        'gi' -- Replace non-alphanumeric characters with hyphens
                )
           )::slug;
end
$$;

create type measurement_system as enum (
    'imperial',
    'metric'
    );



create domain weight as bigint
    constraint weight_gt_zero check (value is null or value > 0);

comment on domain weight is $$
    Weight in grams.
$$;


create type mass_unit as enum (
    'g',
    'kg',
    'oz',
    'lb'
    );

create domain hex_color as text
    constraint hex_color_valid check (value ~ '^#[a-z0-9]{6}$');

create domain rgb as int[3];

create domain quantity as int
    constraint quantity_gt_zero check (value > 0);

comment on domain quantity is $$
    A quantity of an item. Positive non-zero integer.
$$;