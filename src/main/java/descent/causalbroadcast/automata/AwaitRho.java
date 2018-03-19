package descent.causalbroadcast.automata;

import java.util.ArrayList;

import descent.causalbroadcast.messages.IMControlMessage;
import descent.causalbroadcast.messages.MReliableBroadcast;
import descent.causalbroadcast.messages.MRho;

public class AwaitRho extends AAutomata {

	public AwaitRho(ArrayList<MReliableBroadcast> bufferAlpha, ArrayList<MReliableBroadcast> bufferPi) {
		this.state = EState.AWAIT_RHO;
		this.bufferAlpha = bufferAlpha;
		this.bufferPi = bufferPi;
	}

	@Override
	public AAutomata input(IMControlMessage m) {
		MRho message = (MRho) m;
		return null;
	}

}
