package descent.causalbroadcast.messages;

import peersim.core.Node;

/**
 * Interface of control messages sent by the preventive reliable causal
 * broadcast
 */
public interface IMControlMessage {

	public Node getFrom();

	public Node getTo();

	public Node getSender();

	public Node getReceiver();

}
