package descent.causalbroadcast.automata;

import java.util.ArrayList;

import descent.causalbroadcast.messages.IMControlMessage;
import descent.causalbroadcast.messages.MBuffer;
import descent.causalbroadcast.messages.MReliableBroadcast;
import descent.causalbroadcast.messages.MRho;

public class AwaitSecondBuffer extends AAutomata {

	public AwaitSecondBuffer(ArrayList<MReliableBroadcast> bufferAlpha, ArrayList<MReliableBroadcast> bufferPi) {
		this.state = EState.AWAIT_SECOND_BUFFER;
		this.bufferAlpha = bufferAlpha;
		this.bufferPi = bufferPi;
	}

	@Override
	public AAutomata input(IMControlMessage m) {
		MBuffer message = (MBuffer) m;
		return null;
	}

}
