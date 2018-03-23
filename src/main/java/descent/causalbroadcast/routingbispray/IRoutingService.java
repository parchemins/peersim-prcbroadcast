package descent.causalbroadcast.routingbispray;

import java.util.ArrayList;
import java.util.HashSet;

import descent.causalbroadcast.messages.MAlpha;
import descent.causalbroadcast.messages.MBeta;
import descent.causalbroadcast.messages.MPi;
import descent.causalbroadcast.messages.MReliableBroadcast;
import peersim.core.Node;

/**
 * Service provided by peer-sampling service to route control messages from a
 * process to another process using safe links.
 */
public interface IRoutingService {

	public void sendAlpha(MConnectTo m);

	public void sendBeta(MAlpha m);

	public void sendPi(MBeta m);

	public void sendRho(MPi m);

	public void sendBuffer(Node dest, Node from, Node to, ArrayList<MReliableBroadcast> buffer);

	public void sendToOutview(MReliableBroadcast m);

	public HashSet<Node> getOutview();
}
