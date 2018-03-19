package descent.causalbroadcast.automata;

import java.util.ArrayList;

import descent.causalbroadcast.messages.IMControlMessage;
import descent.causalbroadcast.messages.MBeta;
import descent.causalbroadcast.messages.MReliableBroadcast;

public class AwaitBeta extends AAutomata {

	public AwaitBeta(ArrayList<MReliableBroadcast> bufferAlpha, ArrayList<MReliableBroadcast> bufferPi) {
		this.state = EState.AWAIT_BETA;
		this.bufferAlpha = bufferAlpha;
		this.bufferPi = bufferPi;
	}

	@Override
	public AAutomata input(IMControlMessage m) {
		MBeta message = (MBeta) m;
		return new AwaitRho(this.bufferAlpha, this.bufferPi);
	}

}
