package descent.transport;

import peersim.config.Configuration;
import peersim.core.CommonState;
import peersim.core.Node;
import peersim.edsim.EDSimulator;
import peersim.transport.Transport;

public class IncreasingLatencyTransport implements Transport {

	private static final String PAR_MIN = "min";
	private static Integer MIN;

	private static final String PAR_INC = "inc";
	private static Integer INC;

	private static final String PAR_FROM = "from";
	private static long FROM;

	private static final String PAR_STEP = "step";
	private static long STEP;

	private static final String PAR_STOP = "stop";
	private static long STOP;

	public IncreasingLatencyTransport(String prefix) {
		IncreasingLatencyTransport.MIN = Configuration.getInt(prefix + "." + IncreasingLatencyTransport.PAR_MIN, 0);
		IncreasingLatencyTransport.INC = Configuration.getInt(prefix + "." + IncreasingLatencyTransport.PAR_INC, 0);
		IncreasingLatencyTransport.FROM = Configuration.getLong(prefix + "." + IncreasingLatencyTransport.PAR_FROM, 0);
		IncreasingLatencyTransport.STEP = Configuration.getLong(prefix + "." + IncreasingLatencyTransport.PAR_STEP, 1);
		IncreasingLatencyTransport.STOP = Configuration.getLong(prefix + "." + IncreasingLatencyTransport.PAR_STOP);
	}

	public long getLatency(Node src, Node dest) {
		Long time = CommonState.getTime();
		if (CommonState.getTime() > IncreasingLatencyTransport.STOP)
			time = IncreasingLatencyTransport.STOP;
		if (CommonState.getTime() < IncreasingLatencyTransport.FROM)
			return IncreasingLatencyTransport.MIN;

		long nbStep = (time - IncreasingLatencyTransport.FROM) / IncreasingLatencyTransport.STEP;
		if (nbStep > 0) {
			return nbStep * IncreasingLatencyTransport.INC + IncreasingLatencyTransport.MIN;
		} else {
			return 0;
		}
	}

	public void send(Node src, Node dest, Object msg, int pid) {
		EDSimulator.add(getLatency(src, dest), msg, dest, pid);
	}

	public Object clone() {
		return this;
	}

}
