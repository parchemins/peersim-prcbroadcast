package descent.causalbroadcast.automata;

import java.util.ArrayList;

import descent.causalbroadcast.messages.IMControlMessage;
import descent.causalbroadcast.messages.MReliableBroadcast;

public class Initialize extends AAutomata {

	public AAutomata nextState;

	public Initialize(AAutomata nextState) {
		this.state = EState.INITIALIZE;
		this.bufferAlpha = new ArrayList<MReliableBroadcast>();
		this.bufferPi = new ArrayList<MReliableBroadcast>();

		this.nextState = nextState;
	}

	@Override
	public AAutomata input(IMControlMessage m) {
		return nextState;
	}
}
