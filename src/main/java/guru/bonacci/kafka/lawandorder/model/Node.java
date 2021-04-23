package guru.bonacci.kafka.lawandorder.model;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Node {

    public String id;
    public String parentId;
}
