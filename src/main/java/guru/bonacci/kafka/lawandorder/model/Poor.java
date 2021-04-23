package guru.bonacci.kafka.lawandorder.model;

import java.util.List;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.ToString;

@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Poor {

    public String id;
    public List<String> fkIds;
    
    public List<PoorAndFlat> toPaf() {
    	return fkIds.stream()
    				.map(fk -> new PoorAndFlat(id, fk))
    				.collect(Collectors.toList());
    }
}
