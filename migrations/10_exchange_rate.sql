create table exchange_rate
(
    source currency_code not null,
    target currency_code not null,
    rate   numeric       not null,
    time   timestamptz   not null default now(),

    primary key (source, target)
);

create or replace function convert_currency(p_source currency, p_target currency_code) returns currency
    language plpgsql
    stable as
$$
declare
    v_output currency;
begin

    if p_source.currency_code = p_target then
        return p_source;
    end if;

    select p_source.amount * er.rate,
           p_target
    into strict v_output
    from exchange_rate er
    where er.source = p_source.currency_code
      and er.target = p_target;

    return v_output;
end;
$$;