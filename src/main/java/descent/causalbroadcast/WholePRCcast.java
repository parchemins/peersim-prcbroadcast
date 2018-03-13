package descent.causalbroadcast;

import descent.causalbroadcast.messages.IMControlMessage;
import descent.causalbroadcast.messages.MAlpha;
import descent.causalbroadcast.messages.MBeta;
import descent.causalbroadcast.messages.MBuffer;
import descent.causalbroadcast.messages.MPi;
import descent.causalbroadcast.messages.MRho;
import descent.causalbroadcast.routingbispray.MConnectTo;
import descent.causalbroadcast.routingbispray.MRemoveRoute;
import descent.causalbroadcast.routingbispray.SprayWithRouting;
import peersim.cdsim.CDProtocol;
import peersim.core.Node;
import peersim.edsim.EDProtocol;

// Whole protocol at once because peersim does not handle inheritance very well.
public class WholePRCcast implements EDProtocol, CDProtocol {

	public BiPreventiveReliableCausalBroadcast prcb;
	public SprayWithRouting swr;

	public WholePRCcast(String prefix) {
		this.prcb = new BiPreventiveReliableCausalBroadcast(prefix);
		this.swr = new SprayWithRouting(this.prcb);
	}

	public WholePRCcast() {
		this.prcb = new BiPreventiveReliableCausalBroadcast();
		this.swr = new SprayWithRouting(this.prcb);
	}

	public void nextCycle(Node node, int protocolId) {
		this.swr.periodicCall();
	}

	public void processEvent(Node node, int protocolId, Object message) {
		// Give the message to the proper sub-protocol
		if (message instanceof MConnectTo) {
			// (TODO)
		} else if (message instanceof MRemoveRoute) {
			MRemoveRoute m = (MRemoveRoute) message;
			this.swr.removeRoute(m.from, m.to);
		} else if (message instanceof IMControlMessage) {
			IMControlMessage imcm = (IMControlMessage) message;
			if (imcm.getReceiver() == prcb.node) {
				if (message instanceof MAlpha) {
					prcb.receiveAlpha(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MBeta) {
					prcb.receiveBeta(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MPi) {
					prcb.receivePi(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MRho) {
					prcb.receiveRho(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MBuffer) {
					prcb.receiveBuffer(imcm.getFrom(), imcm.getTo(), ((MBuffer) message).messages);
				} else if (message instanceof MConnectTo) {
					swr.receiveMConnectTo(imcm.getFrom(), imcm.getTo(), ((MConnectTo) message).mediator);
				}
			} else {
				if (message instanceof MAlpha) {
					swr.sendAlpha(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MBeta) {
					swr.sendBeta(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MPi) {
					swr.sendPi(imcm.getFrom(), imcm.getTo());
					swr.removeRoute(imcm.getFrom(), imcm.getTo());
				} else if (message instanceof MRho) {
					swr.sendRho(imcm.getFrom(), imcm.getTo());
					swr.removeRoute(imcm.getFrom(), imcm.getTo());
				}
			}

		}
	}

	@Override
	public Object clone() {
		// TODO maybe clone some configurations
		return new WholePRCcast();
	}

}
