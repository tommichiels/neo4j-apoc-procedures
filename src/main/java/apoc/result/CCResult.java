package apoc.result;

import java.util.List;
import java.util.Map;

/**
 * @author mh
 * @since 26.02.16
 */
public class CCResult {
    public final Long ccId;
	public final List<Long> nodeIds;
    public final Map stats;
	
    
    public CCResult(Long ccId,List<Long> nodeIds, Map stats) {
		this.ccId = ccId;
    	this.nodeIds = nodeIds;
		this.stats = stats;
	}
    
}
