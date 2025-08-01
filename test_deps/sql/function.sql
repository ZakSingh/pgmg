CREATE OR REPLACE FUNCTION test_func() RETURNS text AS $$
BEGIN
    RETURN 'hello';
END;
$$ LANGUAGE plpgsql;