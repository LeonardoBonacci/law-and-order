package guru.bonacci.kafka.lawandorder.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
public class NestedNode {

    public String id;
    public String parentId;
    public NestedNode parent;
    
    public static NestedNode from(Node node) {
    	NestedNode pn = new NestedNode();
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
