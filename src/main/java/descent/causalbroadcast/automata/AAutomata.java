package descent.causalbroadcast.automata;

import java.util.ArrayList;

import descent.causalbroadcast.messages.IMControlMessage;
import descent.causalbroadcast.messages.MReliableBroadcast;

public abstract class AAutomata {

	public EState state;

	public ArrayList<MReliableBroadcast> bufferAlpha;
	public ArrayList<MReliableBroadcast> bufferPi;

	public abstract AAutomata input(IMControlMessage m);
}
