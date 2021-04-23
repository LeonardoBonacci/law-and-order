package guru.bonacci.kafka.lawandorder.model;

import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
@RegisterForReflection
public class NodeWrapper {

    public PathNode pnode;
    public boolean parentIsProcessed;
    
    public static NodeWrapper from(PathNode pnode) {
    	NodeWrapper wrap = new NodeWrapper();
    	wrap.pnode = pnode;		
    	wrap.parentIsProcessed = false; //default
    	return wrap;
    }
}
