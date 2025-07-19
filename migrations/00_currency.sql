create type currency_code as enum ( 'usd', 'gbp', 'cad' );

create type currency as
(
    amount        int,
    currency_code currency_code
);

comment on type currency is $$
  An amount with a currency.
$$;

comment on column currency.amount is $$
  @pgrpc_not_null
  Amount in the smallest unit of the currency.
$$;

comment on column currency.currency_code is '@pgrpc_not_null';

create function currency_mul_int(
    c currency,
    n int
)
    returns currency
    language sql
    immutable
as
$$
select row (c.amount * n, c.currency_code)::currency;
$$;

create operator * (
    leftarg = currency,
    rightarg = int,
    function = currency_mul_int
    );

create function currency_mul_numeric(
    c currency,
    n numeric
)
    returns currency
    language sql
    immutable
as
$$
select row (c.amount::numeric * n, c.currency_code)::currency;
$$;

create operator * (
    leftarg = currency,
    rightarg = numeric,
    function = currency_mul_numeric
    );

create function currency_add(
    a currency,
    b currency
) returns currency as $$
begin
    if a.currency_code != b.currency_code then
        raise exception 'Currency codes must match: % vs %', a.currency_code, b.currency_code;
    end if;
    return (a.amount + b.amount, a.currency_code)::currency;
end;
$$ language plpgsql immutable;

create function currency_sub(
    a currency,
    b currency
) returns currency as $$
begin
    if a.currency_code != b.currency_code then
        raise exception 'Currency codes must match: % vs %', a.currency_code, b.currency_code;
    end if;
    return (a.amount - b.amount, a.currency_code)::currency;
end;
$$ language plpgsql immutable;

create function currency_mul(
    a currency,
    b currency
) returns currency as $$
begin
    if a.currency_code != b.currency_code then
        raise exception 'Currency codes must match: % vs %', a.currency_code, b.currency_code;
    end if;
    return (a.amount * b.amount, a.currency_code)::currency;
end;
$$ language plpgsql immutable;

create function currency_div(
    a currency,
    b currency
) returns currency as $$
begin
    if a.currency_code != b.currency_code then
        raise exception 'Currency codes must match: % vs %', a.currency_code, b.currency_code;
    end if;
    if b.amount = 0 then
        raise exception 'Division by zero';
    end if;
    return (a.amount / b.amount, a.currency_code)::currency;
end;
$$ language plpgsql immutable;

create operator + (
    function = currency_add,
    leftarg = currency,
    rightarg = currency
    );

create operator - (
    function = currency_sub,
    leftarg = currency,
    rightarg = currency
    );

create operator * (
    function = currency_mul,
    leftarg = currency,
    rightarg = currency
    );

create operator / (
    function = currency_div,
    leftarg = currency,
    rightarg = currency
    );


-- First, create a state function that handles accumulating currencies
create or replace function currency_sum_sfunc(
    state currency,
    value currency
)
    returns currency
    language plpgsql
    immutable as
$$
begin
    -- If the state is null (first value), just return the value
    if state is null then
        return value;
    end if;

    -- Ensure both currencies have the same currency_code
    if state.currency_code <> value.currency_code then
        raise exception 'Cannot sum currencies with different currency codes: % and %',
            state.currency_code, value.currency_code;
    end if;

    -- Add the amounts and keep the currency code
    return (state.amount + value.amount, state.currency_code)::currency;
end;
$$;

-- Create the sum aggregate function
create aggregate sum(currency) (
    sfunc = currency_sum_sfunc,
    stype = currency,
    parallel = safe
    );

comment on aggregate sum(currency) is 'Sum currency values. All currencies must have the same currency code.';
