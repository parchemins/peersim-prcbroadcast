package descent.causalbroadcast.messages;

import descent.rps.IMessage;
import peersim.core.Node;

/**
 * Message produced and consumed by reliable broadcast.
 */
public class MReliableBroadcast implements IMessage {

	public final Node origin;
	public final Integer counter;
	public final IMessage payload;

	public final Node sender;

	public MReliableBroadcast(Node sender, Node origin, Integer counter, IMessage payload) {
		this.sender = sender;
		this.origin = origin;
		this.counter = counter;
		this.payload = payload;
	}

	public Object getPayload() {
		return this.payload;
	}

	public MReliableBroadcast copy(Node sender) {
		return new MReliableBroadcast(sender, this.origin, this.counter, this.payload);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((counter == null) ? 0 : counter.hashCode());
		result = prime * result + ((origin == null) ? 0 : origin.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		MReliableBroadcast other = (MReliableBroadcast) obj;
		if (counter == null) {
			if (other.counter != null)
				return false;
		} else if (!counter.equals(other.counter))
			return false;
		if (origin == null) {
			if (other.origin != null)
				return false;
		} else if (!origin.equals(other.origin))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "MRB [origin=" + origin.getID() + ", counter=" + counter + ", sender=" + sender.getID()
				+ "]";
	}

}
