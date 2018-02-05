package descent.broadcast.causal.prcbroadcast;

import java.util.ArrayList;

import descent.broadcast.reliable.MReliableBroadcast;
import peersim.core.Node;

/**
 * Buffer containing broadcast messages either to deliver, to ignore.
 */
public class MBuffer {

	public final Node from;
	public final Node to;
	public final ArrayList<MReliableBroadcast> messages;

	public MBuffer(Node from, Node to, ArrayList<MReliableBroadcast> messages) {
		this.from = from;
		this.to = to;
		this.messages = messages;
	}

}
