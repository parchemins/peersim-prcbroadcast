package descent.causalbroadcast;

import descent.causalbroadcast.routingbispray.MConnectTo;
import peersim.core.Node;

public interface IPRCB {

	public void open(MConnectTo m, boolean bypassSafety);

	public boolean isSafe(Node neighbor);

	public boolean isYetToBeSafe(Node neighbor);

	public boolean isNotSafe(Node neighbor);

	public boolean isStillChecking(Node neighbor);

	public void close(Node to);

	public void setNode(Node n);
}
