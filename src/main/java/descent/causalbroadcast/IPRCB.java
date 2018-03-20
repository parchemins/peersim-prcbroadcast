package descent.causalbroadcast;

import peersim.core.Node;

public interface IPRCB {

	public void open(Node to, boolean bypassSafety);

	public boolean isSafe(Node neighbor);

	public boolean isYetToBeSafe(Node neighbor);

	public boolean isNotSafe(Node neighbor);

	public void close(Node to);
}
