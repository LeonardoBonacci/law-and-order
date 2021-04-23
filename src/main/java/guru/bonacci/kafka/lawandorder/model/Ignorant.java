package guru.bonacci.kafka.lawandorder.model;

import java.util.List;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Ignorant {

    public String id;
    public List<String> fkIds;
    
    public List<IgnorantFlat> flat() {
    	return fkIds.stream()
    				.map(fk -> new IgnorantFlat(id, fk))
    				.collect(Collectors.toList());
    }
}
