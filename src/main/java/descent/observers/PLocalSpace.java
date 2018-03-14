package descent.observers;

import descent.observers.structure.DictGraph;
import descent.observers.structure.IObserverProgram;
import peersim.config.Configuration;

public class PLocalSpace implements IObserverProgram {

	private final static String PAR_PREFIX = "prefix";
	private static String PREFIX;

	public PLocalSpace(String prefix) {
		PLocalSpace.PREFIX = Configuration.getString(prefix + "." + PLocalSpace.PAR_PREFIX, "");
	}

	public void tick(long currentTick, DictGraph observer) {
		// (TODO)
		// System.out.println(PLocalSpace.PREFIX+" " + );
	}

	public void onLastTick(DictGraph observer) {
	}

}
