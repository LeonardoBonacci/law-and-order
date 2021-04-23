package guru.bonacci.kafka.lawandorder.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
public class NodeWrapper {

    public NestedNode pnode;
    public boolean parentIsProcessed;
    
    public static NodeWrapper from(NestedNode pnode) {
    	NodeWrapper wrap = new NodeWrapper();
    	wrap.pnode = pnode;		
    	wrap.parentIsProcessed = false; //default
    	return wrap;
    }
}
