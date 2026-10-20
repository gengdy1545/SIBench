CREATE FUNCTION public.sibench_row_capture() RETURNS trigger
AS :'capture_library', 'sibench_row_capture'
LANGUAGE C;
CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON savingaccount
FOR EACH ROW EXECUTE FUNCTION public.sibench_row_capture();
CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON transfer
FOR EACH ROW EXECUTE FUNCTION public.sibench_row_capture();
CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON customer
FOR EACH ROW EXECUTE FUNCTION public.sibench_row_capture();
CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON company
FOR EACH ROW EXECUTE FUNCTION public.sibench_row_capture();
CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON checkingaccount
FOR EACH ROW EXECUTE FUNCTION public.sibench_row_capture();
CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON checking
FOR EACH ROW EXECUTE FUNCTION public.sibench_row_capture();
CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON loanapps
FOR EACH ROW EXECUTE FUNCTION public.sibench_row_capture();
CREATE TRIGGER sibench_row_capture AFTER INSERT OR UPDATE OR DELETE ON loantrans
FOR EACH ROW EXECUTE FUNCTION public.sibench_row_capture();
