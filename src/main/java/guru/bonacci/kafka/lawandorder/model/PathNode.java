package guru.bonacci.kafka.lawandorder.model;

import io.quarkus.runtime.annotations.RegisterForReflection;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
@RegisterForReflection
public class PathNode {

    public String id;
    public String parentId;
    public PathNode parent;
    
    public static PathNode from(Node node) {
    	PathNode pn = new PathNode();
    	pn.id = node.id;		
    	pn.parentId = node.parentId;		
    	pn.parent = null;
    	return pn;
    }
    
    // warning: terrible code below..
    public Node toNode() {
    	Node n = new Node();
    	n.id = id;		
    	n.parentId = parentId;
    	return n;
    }
}
