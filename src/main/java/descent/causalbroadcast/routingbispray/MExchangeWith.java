package descent.causalbroadcast.routingbispray;

import descent.causalbroadcast.messages.IMControlMessage;
import descent.rps.IMessage;
import peersim.core.Node;

/**
 * Message that informs the counterpart process to give some of its partial view
 * to the originator of the message.
 */
public class MExchangeWith implements IMessage, IMControlMessage {

	public final Node from;
	public final Node to;
	public final Integer nbReferences;

	public MExchangeWith(Node from, Node to, Integer nbReferences) {
		this.from = from;
		this.to = to;
		this.nbReferences = nbReferences;
	}

	public Node getMediator() {
		return null;
	}

	public Integer getPayload() {
		return this.nbReferences;
	}

	public Node getFrom() {
		return this.from;
	}

	public Node getTo() {
		return this.to;
	}

	public Node getSender() {
		return this.to;
	}

	public Node getReceiver() {
		return this.from;
	}

}
