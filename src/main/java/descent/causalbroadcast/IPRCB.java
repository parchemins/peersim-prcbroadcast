package descent.causalbroadcast;

import peersim.core.Node;

public interface IPRCB {

	public void openO(Node to, boolean bypassSafety);

	public void openI(Node to, boolean bypassSafety);

	public boolean isSafe(Node neighbor);

	public boolean isYetToBeSafe(Node neighbor);

	public boolean isNotSafe(Node neighbor);

	public void close(Node to);
}
