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

    public Node node;
    public boolean parentIsProcessed;
    
    public static NodeWrapper from(Node node) {
    	NodeWrapper n = new NodeWrapper();
    	n.node = node;		
    	return n;
    }
}
