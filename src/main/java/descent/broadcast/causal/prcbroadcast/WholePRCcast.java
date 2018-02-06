package descent.broadcast.causal.prcbroadcast;

import descent.broadcast.causal.prcbroadcast.routing.MConnectTo;
import descent.broadcast.causal.prcbroadcast.routing.SprayWithRouting;
import peersim.cdsim.CDProtocol;
import peersim.core.Node;
import peersim.edsim.EDProtocol;

public class WholePRCcast implements EDProtocol, CDProtocol {

	public PreventiveReliableCausalBroadcast prcb;
	public SprayWithRouting swr;

	public WholePRCcast() {
		// TODO Auto-generated constructor stub
	}

	public void nextCycle(Node arg0, int arg1) {
		// TODO Auto-generated method stub

	}

	public void processEvent(Node node, int processId, Object message) {
		// Give the message to the proper sub-protocol
		if (message instanceof IMControlMessage) {
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
				} else if (message instanceof MRho) {
					swr.sendRho(imcm.getFrom(), imcm.getTo());
				}
			}

		}

		if (message instanceof MAlpha) {
			MAlpha ma = (MAlpha) message;
			if (ma.to == prcb.node) {
				prcb.receiveAlpha(ma.from, ma.to);
			} else {
				swr.sendAlpha(ma.from, ma.to);
			}
		} else if (message instanceof MBeta) {
			MBeta mb = (MBeta) message;
			if (mb.from == prcb.node) {
				prcb.receiveBeta(mb.from, mb.to);
			}
		}
	}

	@Override
	public Object clone() {
		// TODO Auto-generated method stub
		return new WholePRCcast();
	}

}
