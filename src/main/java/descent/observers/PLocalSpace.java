package descent.observers;

import java.util.ArrayList;
import java.util.HashSet;

import descent.causalbroadcast.PRCBcast;
import descent.causalbroadcast.WholePRCcast;
import descent.causalbroadcast.routingbispray.SprayWithRouting;
import descent.controllers.CDynamicNetwork;
import descent.observers.structure.DictGraph;
import descent.observers.structure.IObserverProgram;
import descent.observers.structure.Stats;
import peersim.core.Node;

public class PLocalSpace implements IObserverProgram {

	public PLocalSpace() {

	}

	public void tick(long currentTick, DictGraph observer) {
		ArrayList<Double> unsafe = new ArrayList<Double>();
		ArrayList<Double> safe = new ArrayList<Double>();

		ArrayList<Double> routes = new ArrayList<Double>();

		ArrayList<Double> duplicatesRouteSafe = new ArrayList<Double>();

		for (Node n : CDynamicNetwork.networks.get(0)) {
			PRCBcast prcb = ((WholePRCcast) n.getProtocol(WholePRCcast.PID)).prcb;
			SprayWithRouting swr = ((WholePRCcast) n.getProtocol(WholePRCcast.PID)).swr;

			unsafe.add((double) prcb.unsafe.size());
			safe.add((double) prcb.safe.size());

			routes.add((double) swr.routes.inUse().size());

			HashSet<Node> view = new HashSet<Node>(prcb.safe);
			view.retainAll(swr.routes.inUse());

			duplicatesRouteSafe.add((double) view.size());
		}

		Stats sUnsafe = Stats.getFromSmall(unsafe);
		Stats sRoutes = Stats.getFromSmall(routes);
		Stats sSafe = Stats.getFromSmall(safe);
		Stats sDuplicates = Stats.getFromSmall(duplicatesRouteSafe);
		System.out.println("PLS. " + sUnsafe.mean + "  " + sSafe.mean + "  " + sRoutes.mean + " " + sDuplicates.mean);
	}

	public void onLastTick(DictGraph observer) {
	}

}
