create extension citext;
create extension ltree;
create extension btree_gist;
create extension unaccent;
create extension pg_trgm;
create extension pgcrypto;

do $$
    begin
        if current_database() = 'miniswap' then
            create extension pg_cron;
        end if;
    end
$$;