CREATE OR REPLACE FUNCTION test_trigger_func() RETURNS trigger AS $$
BEGIN
    PERFORM test_func();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER test_trigger
AFTER INSERT ON pg_description
FOR EACH ROW EXECUTE FUNCTION test_trigger_func();