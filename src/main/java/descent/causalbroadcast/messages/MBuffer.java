package descent.causalbroadcast.messages;

import java.util.ArrayList;

import peersim.core.Node;

/**
 * Buffer containing broadcast messages either to deliver, to ignore.
 */
public class MBuffer implements IMControlMessage {

	public final Node from;
	public final Node to;

	public final Node sender;
	public final Node receiver;

	public final ArrayList<MReliableBroadcast> messages;

	public MBuffer(Node from, Node to, Node sender, Node receiver, ArrayList<MReliableBroadcast> messages) {
		this.from = from;
		this.to = to;
		this.sender = sender;
		this.receiver = receiver;
		this.messages = messages;
	}
	
	public Node getMediator() {
		return null;
	}

	public Node getFrom() {
		return this.from;
	}

	public Node getTo() {
		return this.to;
	}

	public Node getSender() {
		return this.sender;
	}

	public Node getReceiver() {
		return this.receiver;
	}
}
