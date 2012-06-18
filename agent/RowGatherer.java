package agent;

public interface RowGatherer {
	public void init(String tableName) throws Exception;
    public RowBlock read() throws Exception;
    public void close() throws Exception;
}
